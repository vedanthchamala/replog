//! The Stage 5 torture harness: a seeded fault scheduler drives random
//! `kill -9` and control-plane partitions against a real 3-broker process
//! cluster under continuous load, then an offline checker verifies the SPEC
//! contract from client-observed histories alone.
//!
//! `replog_torture --seed 1 --duration-secs 120` runs one schedule;
//! `replog_torture --verify <dir>` re-checks a saved run's history file.
//! Exit code is nonzero on any contract violation.

use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use replog::checker::History;
use replog::client::{ClusterClient, Connection};
use replog::harness::proxy::TcpProxy;
use replog::harness::rng::SplitMix64;
use replog::harness::{isr_of, leader_of, ClusterSpec, ProcCluster};
use replog::proto::{Acks, ProduceRecord};

const TOPIC: &str = "t";
/// Chaff ids live in the top half of the id space; contract ids count up
/// from zero. They must never collide.
const CHAFF_BASE: u64 = 1 << 63;

struct Args {
    seed: u64,
    duration_secs: u64,
    brokers: usize,
    partitions: u32,
    out_dir: String,
    batch_records: usize,
    pace_ms: u64,
    min_fault_gap_ms: u64,
    max_fault_gap_ms: u64,
    min_heal_ms: u64,
    max_heal_ms: u64,
    verify: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        seed: 1,
        duration_secs: 120,
        brokers: 3,
        partitions: 2,
        out_dir: String::new(),
        batch_records: 5,
        pace_ms: 25,
        min_fault_gap_ms: 800,
        max_fault_gap_ms: 3000,
        min_heal_ms: 500,
        max_heal_ms: 4000,
        verify: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut next = |name: &str| it.next().ok_or(format!("{name} needs a value"));
        let parse = |v: String| v.parse::<u64>().map_err(|e| format!("{e}"));
        match flag.as_str() {
            "--seed" => args.seed = parse(next("--seed")?)?,
            "--duration-secs" => args.duration_secs = parse(next("--duration-secs")?)?,
            "--brokers" => args.brokers = parse(next("--brokers")?)? as usize,
            "--partitions" => args.partitions = parse(next("--partitions")?)? as u32,
            "--out-dir" => args.out_dir = next("--out-dir")?,
            "--batch-records" => args.batch_records = parse(next("--batch-records")?)? as usize,
            "--pace-ms" => args.pace_ms = parse(next("--pace-ms")?)?,
            "--min-fault-gap-ms" => args.min_fault_gap_ms = parse(next("--min-fault-gap-ms")?)?,
            "--max-fault-gap-ms" => args.max_fault_gap_ms = parse(next("--max-fault-gap-ms")?)?,
            "--min-heal-ms" => args.min_heal_ms = parse(next("--min-heal-ms")?)?,
            "--max-heal-ms" => args.max_heal_ms = parse(next("--max-heal-ms")?)?,
            "--verify" => args.verify = Some(next("--verify")?),
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if args.out_dir.is_empty() {
        args.out_dir = format!("target/torture/seed-{}", args.seed);
    }
    if args.brokers < 3 {
        return Err("--brokers must be >= 3 (min_isr=2 needs a survivor pair)".into());
    }
    Ok(args)
}

fn sibling(name: &str) -> Result<std::path::PathBuf, String> {
    let me = std::env::current_exe().map_err(|e| e.to_string())?;
    let path = me.parent().ok_or("current_exe has no parent")?.join(name);
    if !path.exists() {
        return Err(format!("{} not found next to replog_torture", path.display()));
    }
    Ok(path)
}

fn verify_saved(path: &str) -> Result<bool, String> {
    let mut file = std::path::PathBuf::from(path);
    if file.is_dir() {
        file = file.join("history.txt");
    }
    let history = History::load(&file).map_err(|e| e.to_string())?;
    let report = history.verify_from_start();
    println!("{report}");
    Ok(report.is_ok())
}

/// One contract producer: acks=all batches, unique ids, only *acked* ids
/// enter the history.
async fn producer(
    client: Arc<ClusterClient>,
    partition: u32,
    ids: Arc<AtomicU64>,
    history: Arc<Mutex<History>>,
    stop: tokio::sync::watch::Receiver<bool>,
    batch_records: usize,
    pace_ms: u64,
) {
    while !*stop.borrow() {
        let base = ids.fetch_add(batch_records as u64, Ordering::Relaxed);
        let records: Vec<ProduceRecord> = (0..batch_records as u64)
            .map(|i| {
                let mut value = vec![0u8; 100];
                value[..8].copy_from_slice(&(base + i).to_le_bytes());
                ProduceRecord { key: None, value }
            })
            .collect();
        if client
            .produce(TOPIC, partition, Acks::All, records)
            .await
            .is_ok()
        {
            let mut h = history.lock().unwrap();
            for id in base..base + batch_records as u64 {
                h.record_produced(id, TOPIC);
            }
        }
        // Unacked ids are simply skipped: the contract says nothing about
        // them (they may or may not surface — at-least-once, one direction).
        tokio::time::sleep(Duration::from_millis(pace_ms)).await;
    }
}

/// The chaff producer: acks=1 records fired at a *randomly chosen broker*
/// with a stale-tolerant direct connection — when that broker is a zombie
/// leader (cut from the controller, already deposed), these are exactly the
/// divergent-suffix records epoch reconciliation must truncate. Their ids
/// are deliberately outside the contract.
async fn chaff(
    client: Arc<ClusterClient>,
    partitions: u32,
    seed: u64,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut rng = SplitMix64::new(seed ^ 0xC4AF);
    let mut next_chaff = CHAFF_BASE;
    let mut conn: Option<Connection> = None;
    let mut rotate_at = Instant::now();
    while !*stop.borrow() {
        if conn.is_none() || Instant::now() >= rotate_at {
            let brokers = client.metadata().brokers;
            conn = match brokers.is_empty() {
                true => None,
                false => {
                    let (_, addr) = rng.pick(&brokers);
                    Connection::connect(addr).await.ok()
                }
            };
            rotate_at = Instant::now() + Duration::from_secs(2);
        }
        if let Some(c) = &conn {
            let mut value = vec![0u8; 100];
            value[..8].copy_from_slice(&next_chaff.to_le_bytes());
            next_chaff += 1;
            let partition = rng.range(0, partitions as u64) as u32;
            let sent = tokio::time::timeout(
                Duration::from_millis(500),
                c.produce(TOPIC, partition, Acks::Written, vec![ProduceRecord {
                    key: None,
                    value,
                }]),
            )
            .await;
            if !matches!(sent, Ok(Ok(_))) {
                conn = None; // wrong broker, dead broker, or slow: rotate
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// One reader: fetches a partition sequentially from offset 0, recording
/// every consumed record. Two of these per partition make the same-offset ⇒
/// same-id check a real cross-reader comparison.
async fn reader(
    client: Arc<ClusterClient>,
    name: String,
    partition: u32,
    history: Arc<Mutex<History>>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut position = 0u64;
    while !*stop.borrow() {
        match client.fetch(TOPIC, partition, position, 256 << 10, 200).await {
            Ok((_, _, records)) => {
                if records.is_empty() {
                    continue;
                }
                let mut h = history.lock().unwrap();
                for r in &records {
                    let id = u64::from_le_bytes(r.value[..8].try_into().unwrap());
                    h.record_consumed(&name, 0, TOPIC, partition, r.offset, id);
                    position = r.offset + 1;
                }
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

async fn run(args: &Args) -> Result<bool, String> {
    let out_dir = std::path::PathBuf::from(&args.out_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).map_err(|e| e.to_string())?;

    let mut spec = ClusterSpec::new(
        sibling("replog_controller")?,
        sibling("replog_broker")?,
        out_dir.join("cluster"),
    );
    spec.brokers = args.brokers;
    spec.session_timeout_ms = 700;
    spec.replica_lag_ms = 1500;

    // Controller first; then one controller-link proxy per broker — the
    // partition lever — then the brokers, dialing through their proxies.
    let mut cluster = ProcCluster::start_controller_only(spec).map_err(|e| e.to_string())?;
    let mut proxies = Vec::with_capacity(args.brokers);
    for id in 0..args.brokers as u32 {
        let proxy = TcpProxy::start(cluster.controller_addr.clone())
            .await
            .map_err(|e| e.to_string())?;
        cluster
            .add_broker(id, Some(&proxy.addr.clone()))
            .map_err(|e| e.to_string())?;
        proxies.push(proxy);
    }
    let n = args.brokers;
    cluster
        .wait_for_meta("all brokers to register", |m| m.brokers.len() >= n)
        .await;

    let addrs = cluster.broker_addrs();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let client = Arc::new(
        ClusterClient::connect(&addr_refs)
            .await
            .map_err(|e| e.to_string())?,
    );
    client
        .create_topic(TOPIC, args.partitions, 3)
        .await
        .map_err(|e| e.to_string())?;
    for p in 0..args.partitions {
        cluster
            .wait_for_meta("full initial ISR", |m| isr_of(m, TOPIC, p).len() == 3)
            .await;
    }

    let history = Arc::new(Mutex::new(History::new()));
    let ids = Arc::new(AtomicU64::new(0));
    let (stop_producers_tx, stop_producers) = tokio::sync::watch::channel(false);
    let (stop_readers_tx, stop_readers) = tokio::sync::watch::channel(false);
    let mut tasks = Vec::new();
    for p in 0..args.partitions {
        tasks.push(tokio::spawn(producer(
            client.clone(),
            p,
            ids.clone(),
            history.clone(),
            stop_producers.clone(),
            args.batch_records,
            args.pace_ms,
        )));
        for reader_name in ["reader-a", "reader-b"] {
            tasks.push(tokio::spawn(reader(
                client.clone(),
                format!("{reader_name}-{p}"),
                p,
                history.clone(),
                stop_readers.clone(),
            )));
        }
    }
    tasks.push(tokio::spawn(chaff(
        client.clone(),
        args.partitions,
        args.seed,
        stop_producers.clone(),
    )));

    // The seeded schedule: gap → fault → heal-delay → heal → next gap.
    // Sequential on purpose: one active fault at a time (two dead brokers
    // out of three is guaranteed unavailability — it tests nothing new).
    let mut rng = SplitMix64::new(args.seed);
    let mut schedule: Vec<String> = Vec::new();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_secs);
    let log_event = |schedule: &mut Vec<String>, msg: String| {
        let line = format!("[t={:7.2}s] {msg}", started.elapsed().as_secs_f64());
        eprintln!("{line}");
        schedule.push(line);
    };
    let mut kills = 0u32;
    let mut cuts = 0u32;
    while Instant::now() < deadline {
        let gap = rng.range(args.min_fault_gap_ms, args.max_fault_gap_ms);
        tokio::time::sleep(Duration::from_millis(gap)).await;
        if Instant::now() >= deadline {
            break;
        }
        let victim = rng.range(0, args.brokers as u64) as usize;
        let heal_ms = rng.range(args.min_heal_ms, args.max_heal_ms);
        let meta = cluster.controller_meta().await;
        let role = if (0..args.partitions).any(|p| leader_of(&meta, TOPIC, p) == victim as i32) {
            "leader"
        } else {
            "follower"
        };
        if rng.range(0, 2) == 0 {
            kills += 1;
            log_event(
                &mut schedule,
                format!("FAULT kill -9 broker {victim} ({role}), restart in {heal_ms} ms"),
            );
            cluster.kill9(victim);
            tokio::time::sleep(Duration::from_millis(heal_ms)).await;
            cluster.restart(victim).await.map_err(|e| e.to_string())?;
            log_event(&mut schedule, format!("HEAL broker {victim} restarted"));
        } else {
            cuts += 1;
            log_event(
                &mut schedule,
                format!(
                    "FAULT partition broker {victim} ({role}) from controller, heal in {heal_ms} ms"
                ),
            );
            proxies[victim].cut();
            tokio::time::sleep(Duration::from_millis(heal_ms)).await;
            proxies[victim].heal();
            // Wait for the controller to see it again before the next fault,
            // so faults don't silently compound.
            let addr = cluster.brokers[victim].as_ref().unwrap().addr.clone();
            let victim_id = victim as u32;
            cluster
                .wait_for_meta("partitioned broker to re-register", move |m| {
                    m.brokers.iter().any(|(id, a)| *id == victim_id && *a == addr)
                })
                .await;
            log_event(&mut schedule, format!("HEAL broker {victim} link restored"));
        }
    }

    // Shutdown sequence: everything is healed (sequential faults), so wait
    // for full ISR, stop producing, let readers drain, then check.
    log_event(&mut schedule, "schedule done; waiting for full ISR".into());
    for p in 0..args.partitions {
        cluster
            .wait_for_meta("final full ISR", |m| isr_of(m, TOPIC, p).len() == 3)
            .await;
    }
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
                format!("drain timed out with {missing} acked ids unread — checker will fail"),
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    stop_readers_tx.send(true).ok();
    for t in tasks {
        let _ = t.await;
    }

    let history = Arc::try_unwrap(history)
        .map_err(|_| "history still shared")?
        .into_inner()
        .unwrap();
    history
        .save(&out_dir.join("history.txt"))
        .map_err(|e| e.to_string())?;
    std::fs::write(out_dir.join("schedule.log"), schedule.join("\n") + "\n")
        .map_err(|e| e.to_string())?;

    let report = history.verify_from_start();
    println!(
        "torture seed={} duration={}s: {kills} kills, {cuts} partitions, history in {}",
        args.seed,
        args.duration_secs,
        out_dir.display()
    );
    println!("{report}");
    Ok(report.is_ok())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("replog_torture: {e}");
            return ExitCode::FAILURE;
        }
    };
    let outcome = match &args.verify {
        Some(path) => verify_saved(path),
        None => run(&args).await,
    };
    match outcome {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("replog_torture: CONTRACT VIOLATIONS FOUND");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("replog_torture: {e}");
            ExitCode::FAILURE
        }
    }
}
