//! Cross-system fault-injection harness (Stage 6).
//!
//! ```text
//! replog_faults probe  --target redpanda [--detect-ms 1000] [--faults kill,pause,isolate]
//! replog_faults run    --target redpanda --seed 1 --duration-secs 120 --out-dir target/faults/rp-1 [...]
//! replog_faults verify <dir>
//! ```
//!
//! Targets are presets matching the compose files under `deploy/`: the
//! container names, host-published client ports, peers network, and (for
//! Redpanda) admin ports. `probe` proves the fault backend before any
//! verdict is trusted; `run` is the seeded schedule with the offline checker.

use std::process::ExitCode;

use std::path::PathBuf;
use std::sync::Arc;

use replog::torture::cluster::Fault;
use replog::torture::docker::Docker;
use replog::torture::probe;
use replog::torture::schedule::{self, RunConfig};
use replog::torture::workload::AckLevel;

#[derive(Debug, Clone)]
pub struct Preset {
    pub name: &'static str,
    pub containers: Vec<String>,
    pub peers_network: String,
    pub client_addrs: Vec<String>,
    pub admin_addrs: Vec<String>,
    pub node_ids: Vec<i32>,
}

pub fn preset(name: &str) -> Result<Preset, String> {
    match name {
        "redpanda" => Ok(Preset {
            name: "redpanda",
            containers: (0..3).map(|i| format!("rp-{i}")).collect(),
            peers_network: "replog-rp_peers".into(),
            client_addrs: (0..3).map(|i| format!("localhost:{}", 29090 + i)).collect(),
            admin_addrs: (0..3).map(|i| format!("localhost:{}", 29640 + i)).collect(),
            node_ids: vec![0, 1, 2],
        }),
        "replog" => Ok(Preset {
            name: "replog",
            containers: (0..3).map(|i| format!("rl-{i}")).collect(),
            peers_network: "replog-rl_peers".into(),
            client_addrs: (0..3).map(|i| format!("localhost:{}", 29000 + i)).collect(),
            admin_addrs: vec!["localhost:29099".into()],
            node_ids: vec![0, 1, 2],
        }),
        other => Err(format!("unknown target {other:?} (redpanda|replog)")),
    }
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1).cloned())
}

async fn probe_cmd(args: &[String]) -> Result<bool, String> {
    let target_name = arg_value(args, "--target").ok_or("--target is required")?;
    let detect_ms: u64 = arg_value(args, "--detect-ms").unwrap_or("1000".into()).parse().map_err(|_| "--detect-ms")?;
    let faults = Fault::parse_list(&arg_value(args, "--faults").unwrap_or("kill,pause,isolate".into()))?;
    let topic = arg_value(args, "--topic").unwrap_or("probe".into());
    let p = preset(&target_name)?;
    let docker = Docker::new(p.containers.clone(), p.peers_network.clone());
    match p.name {
        #[cfg(feature = "kafka")]
        "redpanda" | "kafka" => {
            use replog::torture::kafka::{KafkaTarget, KafkaTargetSpec};
            let target = KafkaTarget::new(KafkaTargetSpec {
                docker,
                client_addrs: p.client_addrs.clone(),
                admin_addrs: p.admin_addrs.clone(),
                node_ids: p.node_ids.clone(),
            })?;
            let rows = probe::probe(&target, &topic, detect_ms, &faults).await?;
            print!("{}", probe::render(&rows, detect_ms));
            Ok(rows.iter().all(|r| r.pass))
        }
        "replog" => {
            use replog::torture::replog::{ReplogTarget, ReplogTargetSpec};
            let target = ReplogTarget::new(ReplogTargetSpec {
                docker,
                client_addrs: p.client_addrs.clone(),
                controller_addr: p.admin_addrs[0].clone(),
            });
            let rows = probe::probe(&target, &topic, detect_ms, &faults).await?;
            print!("{}", probe::render(&rows, detect_ms));
            Ok(rows.iter().all(|r| r.pass))
        }
        #[cfg(not(feature = "kafka"))]
        "redpanda" | "kafka" => {
            let _ = docker;
            Err("this binary was built without the `kafka` feature (cargo build --features kafka)".into())
        }
        other => Err(format!("no target implementation for {other}")),
    }
}

fn num(args: &[String], flag: &str, default: u64) -> Result<u64, String> {
    match arg_value(args, flag) {
        Some(v) => v.parse().map_err(|_| format!("{flag} needs an integer")),
        None => Ok(default),
    }
}

fn run_config(args: &[String]) -> Result<RunConfig, String> {
    let faults = match arg_value(args, "--faults").unwrap_or("kill,pause,isolate".into()).as_str() {
        "none" => Vec::new(),
        list => Fault::parse_list(list)?,
    };
    let seed = num(args, "--seed", 1)?;
    let target = arg_value(args, "--target").ok_or("--target is required")?;
    Ok(RunConfig {
        topic: arg_value(args, "--topic").unwrap_or(format!("torture-{seed}")),
        partitions: num(args, "--partitions", 2)? as u32,
        replication: 3,
        seed,
        duration_secs: num(args, "--duration-secs", 120)?,
        faults,
        acks: AckLevel::parse(&arg_value(args, "--acks").unwrap_or("all".into()))?,
        batch_records: num(args, "--batch-records", 5)? as usize,
        pace_ms: num(args, "--pace-ms", 25)?,
        value_bytes: num(args, "--value-bytes", 100)? as usize,
        min_gap_ms: num(args, "--min-fault-gap-ms", 800)?,
        max_gap_ms: num(args, "--max-fault-gap-ms", 3000)?,
        min_heal_ms: num(args, "--min-heal-ms", 3000)?,
        max_heal_ms: num(args, "--max-heal-ms", 6000)?,
        detect_ms: num(args, "--detect-ms", 1000)?,
        readers_per_partition: 2,
        out_dir: PathBuf::from(
            arg_value(args, "--out-dir").unwrap_or(format!("target/faults/{target}/seed-{seed}")),
        ),
    })
}

async fn run_cmd(args: &[String]) -> Result<bool, String> {
    let target_name = arg_value(args, "--target").ok_or("--target is required")?;
    let cfg = run_config(args)?;
    let p = preset(&target_name)?;
    let docker = Docker::new(p.containers.clone(), p.peers_network.clone());
    let summary = match p.name {
        #[cfg(feature = "kafka")]
        "redpanda" | "kafka" => {
            use replog::torture::kafka::{KafkaTarget, KafkaTargetSpec, KafkaWorkload};
            let target = Arc::new(KafkaTarget::new(KafkaTargetSpec {
                docker,
                client_addrs: p.client_addrs.clone(),
                admin_addrs: p.admin_addrs.clone(),
                node_ids: p.node_ids.clone(),
            })?);
            let workload = Arc::new(KafkaWorkload::new(&p.client_addrs, cfg.value_bytes)?);
            schedule::run(target, workload, &cfg).await?
        }
        "replog" => {
            use replog::torture::replog::{ReplogTarget, ReplogTargetSpec, ReplogWorkload};
            let target = Arc::new(ReplogTarget::new(ReplogTargetSpec {
                docker,
                client_addrs: p.client_addrs.clone(),
                controller_addr: p.admin_addrs[0].clone(),
            }));
            let workload = Arc::new(ReplogWorkload::new(&p.client_addrs, cfg.value_bytes).await?);
            schedule::run(target, workload, &cfg).await?
        }
        #[cfg(not(feature = "kafka"))]
        "redpanda" | "kafka" => {
            let _ = docker;
            return Err("this binary was built without the `kafka` feature (cargo build --features kafka)".into());
        }
        other => return Err(format!("no target implementation for {other}")),
    };
    println!(
        "run target={} seed={} duration={}s acks={} faults={:?}: history in {}",
        target_name,
        cfg.seed,
        cfg.duration_secs,
        cfg.acks.name(),
        summary.fault_counts,
        cfg.out_dir.display()
    );
    println!("{}", summary.report);
    print!("{}", summary.render_stats());
    std::fs::write(
        cfg.out_dir.join("summary.txt"),
        format!("{}\n{}", summary.report, summary.render_stats()),
    )
    .map_err(|e| e.to_string())?;
    Ok(summary.ok())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(|s| s.as_str()) {
        Some("probe") => probe_cmd(&args[1..]).await,
        Some("run") => run_cmd(&args[1..]).await,
        Some("verify") => match args.get(1) {
            Some(dir) => schedule::verify_saved(dir),
            None => Err("verify needs a directory".into()),
        },
        Some(other) => Err(format!("unknown command {other:?}")),
        None => Err("usage: replog_faults probe|run|verify ...".into()),
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            eprintln!("replog_faults: expectations not met");
            ExitCode::from(1)
        }
        Err(e) => {
            eprintln!("replog_faults: {e}");
            ExitCode::from(2)
        }
    }
}
