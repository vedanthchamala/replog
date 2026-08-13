//! Append benchmark: measures the cost of each fsync policy on this machine.
//!
//! Emits one CSV row to stdout. Run via bench/run_append_bench.sh, which
//! drives the full policy × value-size matrix and collects results.

use std::process::ExitCode;
use std::time::Instant;

use replog::storage::{FsyncPolicy, Log, LogConfig};

struct Args {
    dir: String,
    policy: String,
    records: usize,
    value_bytes: usize,
    header: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        dir: String::new(),
        policy: String::new(),
        records: 10_000,
        value_bytes: 100,
        header: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--dir" => args.dir = it.next().ok_or("--dir needs a value")?,
            "--policy" => args.policy = it.next().ok_or("--policy needs a value")?,
            "--records" => {
                args.records = it
                    .next()
                    .ok_or("--records needs a value")?
                    .parse()
                    .map_err(|e| format!("--records: {e}"))?
            }
            "--value-bytes" => {
                args.value_bytes = it
                    .next()
                    .ok_or("--value-bytes needs a value")?
                    .parse()
                    .map_err(|e| format!("--value-bytes: {e}"))?
            }
            "--header" => args.header = true,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if args.header {
        return Ok(args);
    }
    if args.dir.is_empty() || args.policy.is_empty() {
        return Err("required: --dir <path> --policy <always|os|batch:<bytes>:<ms>>".into());
    }
    Ok(args)
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

const HEADER: &str = "policy,records,value_bytes,elapsed_secs,appends_per_sec,mb_per_sec,\
p50_us,p99_us,p999_us,max_us,final_flush_ms";

fn run(args: &Args) -> Result<String, String> {
    let policy = FsyncPolicy::parse(&args.policy)?;
    let config = LogConfig {
        fsync: policy,
        ..LogConfig::default()
    };
    let mut log = Log::open(&args.dir, config).map_err(|e| e.to_string())?;

    let value: Vec<u8> = (0..args.value_bytes).map(|i| (i * 31) as u8).collect();
    let mut latencies_ns = Vec::with_capacity(args.records);

    let start = Instant::now();
    for _ in 0..args.records {
        let t = Instant::now();
        log.append(None, value.clone()).map_err(|e| e.to_string())?;
        latencies_ns.push(t.elapsed().as_nanos() as u64);
    }
    let flush_start = Instant::now();
    log.flush().map_err(|e| e.to_string())?;
    let final_flush_ms = flush_start.elapsed().as_secs_f64() * 1e3;
    // Elapsed includes the final flush so every policy is measured to the same
    // finish line: all records durable.
    let elapsed = start.elapsed().as_secs_f64();

    latencies_ns.sort_unstable();
    let record_bytes = 4 + 4 + 24 + args.value_bytes;
    let total_mb = (args.records * record_bytes) as f64 / 1e6;
    Ok(format!(
        "{},{},{},{:.3},{:.0},{:.2},{:.1},{:.1},{:.1},{:.1},{:.2}",
        args.policy,
        args.records,
        args.value_bytes,
        elapsed,
        args.records as f64 / elapsed,
        total_mb / elapsed,
        percentile(&latencies_ns, 0.50) as f64 / 1e3,
        percentile(&latencies_ns, 0.99) as f64 / 1e3,
        percentile(&latencies_ns, 0.999) as f64 / 1e3,
        *latencies_ns.last().unwrap_or(&0) as f64 / 1e3,
        final_flush_ms,
    ))
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("append_bench: {e}");
            return ExitCode::FAILURE;
        }
    };
    if args.header {
        println!("{HEADER}");
        return ExitCode::SUCCESS;
    }
    match run(&args) {
        Ok(row) => {
            println!("{row}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("append_bench: {e}");
            ExitCode::FAILURE
        }
    }
}
