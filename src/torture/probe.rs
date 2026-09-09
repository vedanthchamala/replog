//! Checkpoint 1: prove the fault backend does what it claims before any
//! workload trusts it.
//!
//! For each fault, applied to the *current leader* of a replicated
//! partition, the probe checks the effect a client can observe — leadership
//! moves to another broker within a budget, the victim's client port behaves
//! as the fault predicts (dead / frozen / still serving), and the cluster is
//! fully replicated again after the heal — and prints one row per fault. A
//! harness whose faults are silently no-ops produces the most reassuring
//! zero-violation verdicts of all; this table is what stands between that
//! and the numbers we report.

use std::time::{Duration, Instant};

use super::cluster::{Fault, FaultTarget, majority_leaders, views};

#[derive(Debug)]
pub struct ProbeRow {
    pub fault: Fault,
    pub victim: usize,
    /// Time until the metadata API (asked of any live broker) showed a
    /// different leader; None = never within budget.
    pub leader_moved_ms: Option<u64>,
    pub new_leader: i32,
    /// TCP connect to the victim's client port succeeded (~1 s timeout).
    pub victim_connect: bool,
    /// The victim answered a metadata request itself (~0.7 s timeout).
    pub victim_metadata: Option<Vec<i32>>,
    /// isolate only: how long the victim kept reporting *itself* as leader
    /// after peers had elected around it (None = not measured / never did).
    pub zombie_claim_ms: Option<u64>,
    pub healed_ms: Option<u64>,
    pub pass: bool,
    pub notes: Vec<String>,
}

pub async fn probe<T: FaultTarget>(
    target: &T,
    topic: &str,
    detect_ms: u64,
    faults: &[Fault],
) -> Result<Vec<ProbeRow>, String> {
    target.create_topic(topic, 1, 3).await?;
    target.wait_healthy(topic, 1, Duration::from_secs(60)).await?;
    // Sanity bound, not a pass/fail target: a fault that produces no leader
    // change inside this window did not do what it claims. Redpanda's
    // follower election after a half-open partition (pause/isolate) runs
    // pre-vote rounds with multi-second RPC timeouts, so the bound is generous.
    let budget = Duration::from_millis((5 * detect_ms).max(15_000));
    let mut rows = Vec::new();
    for &fault in faults {
        let leaders = majority_leaders(target, topic, 1).await;
        let victim = leaders[0];
        if victim < 0 {
            return Err("no leader before fault".into());
        }
        let victim = victim as usize;
        let mut notes = Vec::new();
        eprintln!("[probe] {} → broker {} ({})", fault.name(), victim, target.broker_name(victim));
        let t0 = Instant::now();
        target.fault(victim, fault).await?;

        // (a) leadership moves, as any live broker reports it.
        let mut leader_moved_ms = None;
        let mut new_leader = -1;
        while t0.elapsed() < budget {
            let per_broker = views(target, topic, 1).await;
            let l = super::cluster::majority(&per_broker, 1)[0];
            if l >= 0 && l as usize != victim {
                leader_moved_ms = Some(t0.elapsed().as_millis() as u64);
                new_leader = l;
                notes.push(format!(
                    "views at move: {}",
                    per_broker
                        .iter()
                        .enumerate()
                        .map(|(i, v)| format!("b{i}={}", v.as_ref().map(|v| v[0].to_string()).unwrap_or("?".into())))
                        .collect::<Vec<_>>()
                        .join(" ")
                ));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // (b) what does the victim's own port do now?
        let victim_connect = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::net::TcpStream::connect(target.client_addr(victim)),
        )
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false);
        let victim_metadata = target.view_via(victim, topic, 1).await.ok();

        // (c) isolate: how long does the zombie keep claiming leadership?
        let mut zombie_claim_ms = None;
        if fault == Fault::Isolate && leader_moved_ms.is_some() {
            let claim_deadline = Instant::now() + budget;
            loop {
                match target.view_via(victim, topic, 1).await {
                    Ok(v) if v[0] == victim as i32 => {}
                    Ok(v) => {
                        zombie_claim_ms = Some(t0.elapsed().as_millis() as u64);
                        notes.push(format!("victim's view after {} ms: leader={}", t0.elapsed().as_millis(), v[0]));
                        break;
                    }
                    Err(e) => {
                        zombie_claim_ms = Some(t0.elapsed().as_millis() as u64);
                        notes.push(format!("victim stopped answering after {} ms: {e}", t0.elapsed().as_millis()));
                        break;
                    }
                }
                if Instant::now() > claim_deadline {
                    notes.push(format!(
                        "victim still claims leadership {} ms after isolate (budget exhausted)",
                        t0.elapsed().as_millis()
                    ));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // heal and wait for full replication again.
        let th = Instant::now();
        target.heal(victim, fault).await?;
        let healed_ms = match target.wait_healthy(topic, 1, Duration::from_secs(90)).await {
            Ok(()) => Some(th.elapsed().as_millis() as u64),
            Err(e) => {
                notes.push(format!("heal: {e}"));
                None
            }
        };

        let expectation = match fault {
            Fault::Kill | Fault::Pause => victim_metadata.is_none(),
            Fault::Isolate => victim_connect && victim_metadata.is_some(),
        };
        if !expectation {
            notes.push(match fault {
                Fault::Isolate => "expected the isolated broker to still answer clients".into(),
                _ => "expected the victim to stop answering metadata".into(),
            });
        }
        let pass = leader_moved_ms.is_some() && expectation && healed_ms.is_some();
        rows.push(ProbeRow {
            fault,
            victim,
            leader_moved_ms,
            new_leader,
            victim_connect,
            victim_metadata,
            zombie_claim_ms,
            healed_ms,
            pass,
            notes,
        });
        // Let the cluster settle so faults don't compound across rows.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(rows)
}

pub fn render(rows: &[ProbeRow], detect_ms: u64) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "fault    victim  leader moved   new  victim port          zombie claim  healed    verdict   (detect timeout {detect_ms} ms, budget {} ms)\n",
        (5 * detect_ms).max(15_000)
    ));
    for r in rows {
        let moved = r.leader_moved_ms.map(|m| format!("{m:>6} ms")).unwrap_or_else(|| "  never  ".into());
        let port = match (&r.victim_connect, &r.victim_metadata) {
            (true, Some(v)) => format!("serving (leader={})", v[0]),
            (true, None) => "connects, no answer".to_string(),
            (false, _) => "refused".to_string(),
        };
        let zombie = r.zombie_claim_ms.map(|m| format!("{m:>6} ms")).unwrap_or_else(|| "    -    ".into());
        let healed = r.healed_ms.map(|m| format!("{m:>6} ms")).unwrap_or_else(|| "  never  ".into());
        s.push_str(&format!(
            "{:<8} {:<7} {:<14} {:<4} {:<20} {:<13} {:<9} {}\n",
            r.fault.name(),
            r.victim,
            moved,
            r.new_leader,
            port,
            zombie,
            healed,
            if r.pass { "PASS" } else { "FAIL" }
        ));
        for n in &r.notes {
            s.push_str(&format!("         note: {n}\n"));
        }
    }
    s
}
