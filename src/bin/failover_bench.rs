//! Failover-time distribution: repeatedly `kill -9` the partition leader of a
//! real 3-broker cluster (child processes) under a continuous acks=all
//! workload, and measure kill → first-post-kill-ack for each kill.
//!
//! That gap is the client-visible unavailability window: liveness detection
//! (controller session timeout) + election + metadata propagation + client
//! retry. Emits one CSV row per kill to stdout; summary stats go to stderr.

use std::process::ExitCode;
use std::time::Instant;

use replog::client::ClusterClient;
use replog::harness::{leader_of, ClusterSpec, ProcCluster};
use replog::proto::{Acks, ProduceRecord};

struct Args {
    kills: usize,
    batch_records: usize,
    value_bytes: usize,
    warmup_batches: usize,
    session_timeout_ms: u64,
    data_root: String,
    header: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        kills: 10,
        batch_records: 20,
        value_bytes: 100,
        warmup_batches: 50,
        session_timeout_ms: 700,
        data_root: "target/bench-data-failover".into(),
        header: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut next = |name: &str| it.next().ok_or(format!("{name} needs a value"));
        match flag.as_str() {
            "--kills" => args.kills = next("--kills")?.parse().map_err(|e| format!("{e}"))?,
            "--batch-records" => {
                args.batch_records = next("--batch-records")?.parse().map_err(|e| format!("{e}"))?
            }
            "--value-bytes" => {
                args.value_bytes = next("--value-bytes")?.parse().map_err(|e| format!("{e}"))?
            }
            "--warmup-batches" => {
                args.warmup_batches =
                    next("--warmup-batches")?.parse().map_err(|e| format!("{e}"))?
            }
            "--session-timeout-ms" => {
                args.session_timeout_ms =
                    next("--session-timeout-ms")?.parse().map_err(|e| format!("{e}"))?
            }
            "--data-root" => args.data_root = next("--data-root")?,
            "--header" => args.header = true,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(args)
}

const HEADER: &str =
    "kill,old_leader,new_leader,session_timeout_ms,batch_records,value_bytes,gap_ms";

/// One acks=all batch of `n` records, ids stamped into the value prefix.
async fn produce_batch(
    client: &ClusterClient,
    value: &[u8],
    next_id: &mut u64,
    n: usize,
) -> Result<(), replog::client::ClientError> {
    let records: Vec<ProduceRecord> = (0..n)
        .map(|_| {
            let mut v = value.to_vec();
            v[..8].copy_from_slice(&next_id.to_le_bytes());
            *next_id += 1;
            ProduceRecord { key: None, value: v }
        })
        .collect();
    client.produce("f", 0, Acks::All, records).await.map(|_| ())
}

/// The bench binaries live next to this one in the target dir.
fn sibling(name: &str) -> Result<std::path::PathBuf, String> {
    let me = std::env::current_exe().map_err(|e| e.to_string())?;
    let dir = me.parent().ok_or("current_exe has no parent")?;
    let path = dir.join(name);
    if !path.exists() {
        return Err(format!("{} not found next to failover_bench", path.display()));
    }
    Ok(path)
}

async fn run(args: &Args) -> Result<(), String> {
    let mut spec = ClusterSpec::new(
        sibling("replog_controller")?,
        sibling("replog_broker")?,
        &args.data_root,
    );
    spec.session_timeout_ms = args.session_timeout_ms;
    spec.replica_lag_ms = 1500;
    let mut cluster = ProcCluster::start(spec).await.map_err(|e| e.to_string())?;

    let addrs = cluster.broker_addrs();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let client = ClusterClient::connect(&addr_refs)
        .await
        .map_err(|e| e.to_string())?;
    client
        .create_topic("f", 1, 3)
        .await
        .map_err(|e| e.to_string())?;
    cluster
        .wait_for_meta("full ISR", |m| {
            m.topics
                .iter()
                .find(|t| t.name == "f")
                .is_some_and(|t| t.partitions[0].isr.len() == 3)
        })
        .await;

    let value: Vec<u8> = (0..args.value_bytes.max(8)).map(|i| (i * 31) as u8).collect();
    let mut next_id = 0u64;

    let mut gaps_ms: Vec<f64> = Vec::with_capacity(args.kills);
    for kill in 1..=args.kills {
        for _ in 0..args.warmup_batches {
            produce_batch(&client, &value, &mut next_id, args.batch_records)
                .await
                .map_err(|e| format!("warmup produce failed: {e}"))?;
        }
        let meta = cluster.controller_meta().await;
        let old_leader = leader_of(&meta, "f", 0);
        if old_leader < 0 {
            return Err("partition offline before kill".into());
        }
        cluster.kill9(old_leader as usize);
        let killed_at = Instant::now();
        // The client's internal retry rides through detection + election;
        // the first Ok after the kill closes the unavailability window.
        let mut attempts = 0;
        loop {
            match produce_batch(&client, &value, &mut next_id, args.batch_records).await {
                Ok(_) => break,
                Err(e) => {
                    attempts += 1;
                    if attempts >= 3 {
                        return Err(format!("no ack after kill {kill}: {e}"));
                    }
                }
            }
        }
        let gap = killed_at.elapsed();
        let meta = cluster.controller_meta().await;
        let new_leader = leader_of(&meta, "f", 0);
        println!(
            "{},{},{},{},{},{},{:.1}",
            kill,
            old_leader,
            new_leader,
            args.session_timeout_ms,
            args.batch_records,
            args.value_bytes,
            gap.as_secs_f64() * 1e3,
        );
        gaps_ms.push(gap.as_secs_f64() * 1e3);

        cluster
            .restart(old_leader as usize)
            .await
            .map_err(|e| e.to_string())?;
        cluster
            .wait_for_meta("ISR to heal to 3", |m| {
                m.topics
                    .iter()
                    .find(|t| t.name == "f")
                    .is_some_and(|t| t.partitions[0].isr.len() == 3)
            })
            .await;
        // Fresh bootstrap addresses for the client's next refresh.
        let _ = client.refresh_metadata().await;
    }

    gaps_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |q: f64| gaps_ms[((gaps_ms.len() - 1) as f64 * q).round() as usize];
    eprintln!(
        "failover gap over {} kills (session_timeout={}ms): min {:.0} ms, p50 {:.0} ms, p90 {:.0} ms, max {:.0} ms",
        gaps_ms.len(),
        args.session_timeout_ms,
        pct(0.0),
        pct(0.5),
        pct(0.9),
        pct(1.0),
    );
    let _ = std::fs::remove_dir_all(&args.data_root);
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("failover_bench: {e}");
            return ExitCode::FAILURE;
        }
    };
    if args.header {
        println!("{HEADER}");
        return ExitCode::SUCCESS;
    }
    match run(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("failover_bench: {e}");
            ExitCode::FAILURE
        }
    }
}
