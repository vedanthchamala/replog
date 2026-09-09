//! The seeded fault schedule, generic over target and workload.
//!
//! Same shape as Stage 5: gap → fault → heal delay → heal → wait for full
//! health → next gap. One victim at a time, so no fault ever lands on a
//! cluster still recovering from the last one, and the seed fully determines
//! the *schedule* (which fault, which broker, what delays) — not the
//! interleaving with real processes, which is the honest limit of process-level
//! fault injection. What is new is that nothing here knows which system is
//! under test: the target applies faults and answers metadata, the workload
//! speaks the protocol, and the checker judges what clients saw.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::checker::{History, Report};
use crate::harness::rng::SplitMix64;

use super::cluster::{Fault, FaultTarget, wait_fully_healthy};
use super::failover::{
    AckClock, FaultOutcome, LeaderWatch, outcome_csv_row, outcomes_csv_header, percentiles,
};
use super::workload::{AckLevel, Reader, Workload};

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub topic: String,
    pub partitions: u32,
    pub replication: u32,
    pub seed: u64,
    pub duration_secs: u64,
    /// Empty = no faults (baseline run).
    pub faults: Vec<Fault>,
    pub acks: AckLevel,
    pub batch_records: usize,
    pub pace_ms: u64,
    pub value_bytes: usize,
    pub min_gap_ms: u64,
    pub max_gap_ms: u64,
    pub min_heal_ms: u64,
    pub max_heal_ms: u64,
    pub detect_ms: u64,
    pub readers_per_partition: usize,
    pub out_dir: PathBuf,
}

pub struct RunSummary {
    pub report: Report,
    pub readers_per_partition: usize,
    /// Times the cluster failed to re-form within the budget after a heal;
    /// > 0 means the schedule was cut short (see schedule.log).
    pub health_incidents: u32,
    pub fault_counts: BTreeMap<&'static str, u32>,
    pub outcomes: Vec<FaultOutcome>,
    pub acked_total: u64,
    pub zero_ack_seconds: u32,
    pub load_seconds: u32,
}

impl RunSummary {
    pub fn ok(&self) -> bool {
        self.report.is_ok()
    }

    /// Failover statistics, grouped by (fault, role): `leader moved` and
    /// `first ack` percentiles in ms.
    pub fn render_stats(&self) -> String {
        let mut s = String::new();
        let mut groups: BTreeMap<(&str, &str), (Vec<u64>, Vec<u64>, usize)> = BTreeMap::new();
        for o in &self.outcomes {
            let g = groups.entry((o.fault, o.role)).or_default();
            if let Some(m) = o.leader_moved_ms {
                g.0.push(m);
            }
            if let Some(a) = o.max_gap_ms {
                g.1.push(a);
            }
            g.2 += 1;
        }
        s.push_str("fault    role      n   leader moved (min/p50/p90/max ms)   largest ack gap in fault window (min/p50/p90/max ms)\n");
        for ((fault, role), (moved, acks, n)) in groups {
            let fmt = |p: Option<(u64, u64, u64, u64, usize)>| match p {
                Some((mn, p50, p90, mx, k)) => format!("{mn}/{p50}/{p90}/{mx} (n={k})"),
                None => "-".to_string(),
            };
            s.push_str(&format!(
                "{:<8} {:<9} {:<3} {:<36} {}\n",
                fault,
                role,
                n,
                fmt(percentiles(moved)),
                fmt(percentiles(acks))
            ));
        }
        s.push_str(&format!(
            "availability: {} of {} load-seconds had zero acks; {} ids acked in total\n",
            self.zero_ack_seconds, self.load_seconds, self.acked_total
        ));
        if self.health_incidents > 0 {
            s.push_str(&format!(
                "WARNING: cluster failed to re-form after a heal {} time(s); fault injection was cut short (see schedule.log)\n",
                self.health_incidents
            ));
        }
        s.push_str(&format!(
            "duplicates: {} deliveries beyond one per reader per id ({} readers per partition; raw checker count {} includes the extra readers)\n",
            self.extra_duplicates(),
            self.readers_per_partition,
            self.report.duplicate_deliveries
        ));
        s
    }

    /// Deliveries beyond one per (reader, id): with `r` readers per partition
    /// each id is legitimately consumed `r` times, so only what exceeds that
    /// is a retry- or rewind-induced duplicate.
    pub fn extra_duplicates(&self) -> usize {
        self.report
            .consumed_total
            .saturating_sub(self.report.distinct_consumed * self.readers_per_partition)
    }
}

pub async fn run<T: FaultTarget + 'static, W: Workload>(
    target: Arc<T>,
    workload: Arc<W>,
    cfg: &RunConfig,
) -> Result<RunSummary, String> {
    let _ = std::fs::remove_dir_all(&cfg.out_dir);
    std::fs::create_dir_all(&cfg.out_dir).map_err(|e| e.to_string())?;
    let topic = cfg.topic.clone();
    let partitions = cfg.partitions;

    target.create_topic(&topic, partitions, cfg.replication).await?;
    wait_fully_healthy(target.as_ref(), &topic, partitions, Duration::from_secs(90)).await?;

    let started = Instant::now();
    let history = Arc::new(Mutex::new(History::new()));
    let ids = Arc::new(AtomicU64::new(0));
    let clock = Arc::new(AckClock::new(partitions, started));
    let (stop_producers_tx, stop_producers) = tokio::sync::watch::channel(false);
    let (stop_readers_tx, stop_readers) = tokio::sync::watch::channel(false);
    let (stop_watch_tx, stop_watch) = tokio::sync::watch::channel(false);
    let watch = LeaderWatch::spawn(
        target.clone(),
        topic.clone(),
        partitions,
        Duration::from_millis(20),
        stop_watch.clone(),
    );

    let mut tasks = Vec::new();
    for p in 0..partitions {
        let (w, h, ids, clock, stop, t) = (
            workload.clone(),
            history.clone(),
            ids.clone(),
            clock.clone(),
            stop_producers.clone(),
            topic.clone(),
        );
        let (batch, pace, acks) = (cfg.batch_records, cfg.pace_ms, cfg.acks);
        tasks.push(tokio::spawn(async move {
            while !*stop.borrow() {
                let base = ids.fetch_add(batch as u64, Ordering::Relaxed);
                let batch_ids: Vec<u64> = (base..base + batch as u64).collect();
                let acked = w.produce(&t, p, &batch_ids, acks).await;
                if !acked.is_empty() {
                    let mut h = h.lock().unwrap();
                    for id in &acked {
                        h.record_produced(*id, &t);
                    }
                    clock.record(p, acked.len());
                }
                tokio::time::sleep(Duration::from_millis(pace)).await;
            }
        }));
        for r in 0..cfg.readers_per_partition {
            let name = format!("reader-{}-{p}", (b'a' + r as u8) as char);
            let mut reader = workload.reader(&topic, p, &name).await?;
            let (h, stop, t) = (history.clone(), stop_readers.clone(), topic.clone());
            tasks.push(tokio::spawn(async move {
                while !*stop.borrow() {
                    match reader.next().await {
                        Ok(batch) => {
                            if batch.is_empty() {
                                continue;
                            }
                            let mut h = h.lock().unwrap();
                            for (offset, id) in batch {
                                h.record_consumed(&name, 0, &t, p, offset, id);
                            }
                        }
                        Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                    }
                }
            }));
        }
    }

    // Per-second ack timeline (rows: second, partition, acked in that second).
    let timeline: Arc<Mutex<Vec<(u32, u32, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let (clock, timeline, mut stop) = (clock.clone(), timeline.clone(), stop_producers.clone());
        tasks.push(tokio::spawn(async move {
            let mut prev = vec![0u64; clock.partitions() as usize];
            let mut tick = tokio::time::interval_at(
                tokio::time::Instant::from_std(started + Duration::from_secs(1)),
                Duration::from_secs(1),
            );
            let mut sec = 0u32;
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = stop.changed() => return,
                }
                if *stop.borrow() {
                    return;
                }
                sec += 1;
                let mut rows = timeline.lock().unwrap();
                for p in 0..clock.partitions() {
                    let now = clock.acked(p);
                    rows.push((sec, p, now - prev[p as usize]));
                    prev[p as usize] = now;
                }
            }
        }));
    }

    let mut rng = SplitMix64::new(cfg.seed);
    let mut schedule: Vec<String> = Vec::new();
    let log_event = |schedule: &mut Vec<String>, msg: String| {
        let line = format!("[t={:7.2}s] {msg}", started.elapsed().as_secs_f64());
        eprintln!("{line}");
        schedule.push(line);
    };
    let deadline = started + Duration::from_secs(cfg.duration_secs);
    let n = target.broker_count();
    let mut fault_counts: BTreeMap<&'static str, u32> = BTreeMap::new();
    let mut outcomes: Vec<FaultOutcome> = Vec::new();
    let mut health_incidents = 0u32;
    let observe_budget = Duration::from_millis((5 * cfg.detect_ms).max(15_000));

    if cfg.faults.is_empty() {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    }
    while Instant::now() < deadline && !cfg.faults.is_empty() {
        // Tolerate min == max (a fixed cadence, useful for decomposition runs);
        // rng.range requires lo < hi.
        let gap = if cfg.min_gap_ms >= cfg.max_gap_ms {
            cfg.min_gap_ms
        } else {
            rng.range(cfg.min_gap_ms, cfg.max_gap_ms)
        };
        tokio::time::sleep(Duration::from_millis(gap)).await;
        if Instant::now() >= deadline {
            break;
        }
        let victim = rng.range(0, n as u64) as usize;
        let heal_ms = if cfg.min_heal_ms >= cfg.max_heal_ms {
            cfg.min_heal_ms
        } else {
            rng.range(cfg.min_heal_ms, cfg.max_heal_ms)
        };
        let fault = *rng.pick(&cfg.faults);
        let leaders = watch.current();
        let roles: Vec<&'static str> = leaders
            .iter()
            .map(|&l| if l == victim as i32 { "leader" } else { "follower" })
            .collect();
        *fault_counts.entry(fault.name()).or_default() += 1;
        log_event(
            &mut schedule,
            format!(
                "FAULT {} broker {victim} ({}) [{}], heal in {heal_ms} ms",
                fault.name(),
                target.broker_name(victim),
                roles
                    .iter()
                    .enumerate()
                    .map(|(p, r)| format!("p{p}:{r}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        );
        for p in 0..partitions {
            clock.reset_gap(p);
        }
        let issued = Instant::now();
        target.fault(victim, fault).await?;
        // The fault is effective once the docker command has returned; acks
        // that landed while the CLI was running are not post-fault acks.
        let t0 = Instant::now();
        let cli_ms = (t0 - issued).as_millis() as u64;
        let t0_secs = (t0 - started).as_secs_f64();

        let observe = async {
            let mut acks = Vec::new();
            for p in 0..partitions {
                acks.push(
                    clock
                        .first_ack_after(p, t0, t0 + Duration::from_millis(heal_ms) + observe_budget)
                        .await,
                );
            }
            acks
        };
        let heal = async {
            tokio::time::sleep(Duration::from_millis(heal_ms)).await;
            let r = target.heal(victim, fault).await;
            (r, Instant::now())
        };
        let (first_acks, (heal_result, healed_at)) = tokio::join!(observe, heal);
        heal_result?;
        // The fault window closes when acks have resumed after the heal on
        // every partition (or the budget runs out); only then is the largest
        // gap inside it known.
        let resume_deadline = healed_at + observe_budget;
        for p in 0..partitions {
            clock.first_ack_after(p, healed_at, resume_deadline).await;
        }
        let max_gaps: Vec<Option<u64>> = (0..partitions)
            .map(|p| clock.max_gap(p).map(|g| g.as_millis() as u64))
            .collect();
        log_event(
            &mut schedule,
            format!("HEAL {} broker {victim}; waiting for full health", fault.name()),
        );
        let health = wait_fully_healthy(target.as_ref(), &topic, partitions, Duration::from_secs(120)).await;
        let mut cut_short = false;
        match health {
            Ok(()) => log_event(
                &mut schedule,
                format!("HEALTHY after {} ms", healed_at.elapsed().as_millis()),
            ),
            Err(e) => {
                // Do not throw the run away: stop injecting faults, keep the
                // history, and let the checker judge what clients saw. The
                // schedule log records why the run was cut short.
                log_event(
                    &mut schedule,
                    format!("HEALTH TIMEOUT after heal ({e}); stopping fault injection, run is CUT SHORT"),
                );
                cut_short = true;
            }
        }
        for p in 0..partitions {
            let moved = watch.first_new_leader_after(t0, p, victim);
            outcomes.push(FaultOutcome {
                fault: fault.name(),
                victim,
                t0_secs,
                cli_ms,
                heal_after_ms: heal_ms,
                partition: p,
                role: roles[p as usize],
                leader_moved_ms: moved.as_ref().map(|c| (c.at - t0).as_millis() as u64),
                new_leader: moved.as_ref().map(|c| c.new).unwrap_or(-1),
                first_ack_ms: first_acks[p as usize].map(|t| (t - t0).as_millis() as u64),
                max_gap_ms: max_gaps[p as usize],
            });
        }
        if cut_short {
            health_incidents += 1;
            break;
        }
    }

    log_event(&mut schedule, "schedule done; waiting for full health".into());
    if let Err(e) = wait_fully_healthy(target.as_ref(), &topic, partitions, Duration::from_secs(120)).await {
        log_event(&mut schedule, format!("HEALTH TIMEOUT at end of schedule ({e}); draining anyway"));
        health_incidents += 1;
    }
    let load_seconds = started.elapsed().as_secs() as u32;
    stop_producers_tx.send(true).ok();
    log_event(&mut schedule, "producers stopped; readers draining".into());
    let drain_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let missing = {
            let h = history.lock().unwrap();
            let consumed = h.consumed_ids();
            h.produced_ids().difference(&consumed).count()
        };
        if missing == 0 {
            break;
        }
        if Instant::now() >= drain_deadline {
            log_event(
                &mut schedule,
                format!("drain timed out with {missing} acked ids unread — checker will report them"),
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    stop_readers_tx.send(true).ok();
    stop_watch_tx.send(true).ok();
    for t in tasks {
        let _ = t.await;
    }

    let history = Arc::try_unwrap(history)
        .map_err(|_| "history still shared")?
        .into_inner()
        .unwrap();
    history.save(&cfg.out_dir.join("history.txt")).map_err(|e| e.to_string())?;
    std::fs::write(cfg.out_dir.join("schedule.log"), schedule.join("\n") + "\n").map_err(|e| e.to_string())?;
    let mut csv = String::from(outcomes_csv_header());
    csv.push('\n');
    for o in &outcomes {
        csv.push_str(&outcome_csv_row(o));
        csv.push('\n');
    }
    std::fs::write(cfg.out_dir.join("faults.csv"), csv).map_err(|e| e.to_string())?;
    let rows = timeline.lock().unwrap().clone();
    let mut tl = String::from("second,partition,acked\n");
    for (s, p, a) in &rows {
        tl.push_str(&format!("{s},{p},{a}\n"));
    }
    std::fs::write(cfg.out_dir.join("timeline.csv"), tl).map_err(|e| e.to_string())?;
    let mut leaders_log = String::new();
    for c in watch.changes() {
        leaders_log.push_str(&format!(
            "[t={:7.3}s] p{} leader {} -> {}\n",
            (c.at - started).as_secs_f64(),
            c.partition,
            c.old,
            c.new
        ));
    }
    std::fs::write(cfg.out_dir.join("leaders.log"), leaders_log).map_err(|e| e.to_string())?;

    let mut per_second: BTreeMap<u32, u64> = BTreeMap::new();
    for (s, _, a) in &rows {
        if *s <= load_seconds {
            *per_second.entry(*s).or_default() += a;
        }
    }
    let zero_ack_seconds = per_second.values().filter(|&&a| a == 0).count() as u32;
    let acked_total = (0..partitions).map(|p| clock.acked(p)).sum();

    let report = history.verify_from_start();
    Ok(RunSummary {
        report,
        readers_per_partition: cfg.readers_per_partition,
        health_incidents,
        fault_counts,
        outcomes,
        acked_total,
        zero_ack_seconds,
        load_seconds,
    })
}

pub fn verify_saved(path: &str) -> Result<bool, String> {
    let mut file = PathBuf::from(path);
    if file.is_dir() {
        file = file.join("history.txt");
    }
    let history = History::load(&file).map_err(|e| e.to_string())?;
    let report = history.verify_from_start();
    println!("{report}");
    Ok(report.is_ok())
}
