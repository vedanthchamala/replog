//! Broker produce benchmark: throughput and batch-ack latency over TCP as a
//! function of client batch size, ack level, and pipelining depth.
//!
//! Emits one CSV row to stdout; bench/run_broker_bench.sh drives the matrix
//! against a fresh broker process per row.

use std::collections::VecDeque;
use std::process::ExitCode;
use std::time::Instant;

use replog::client::Connection;
use replog::proto::{Acks, ErrorCode, ProduceRecord, Request, Response};

struct Args {
    addr: String,
    records: usize,
    value_bytes: usize,
    batch_records: usize,
    inflight: usize,
    /// Target records/sec for open-loop mode; 0 = closed loop. Open loop
    /// paces batch sends on a fixed schedule and measures ack latency from
    /// the *scheduled* send time, so a backlog counts against latency
    /// instead of silently pausing the load (coordinated omission).
    rate: u64,
    acks: Acks,
    acks_label: String,
    header: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        addr: String::new(),
        records: 100_000,
        value_bytes: 100,
        batch_records: 100,
        inflight: 1,
        rate: 0,
        acks: Acks::Written,
        acks_label: "written".into(),
        header: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut next = |name: &str| it.next().ok_or(format!("{name} needs a value"));
        match flag.as_str() {
            "--addr" => args.addr = next("--addr")?,
            "--records" => args.records = next("--records")?.parse().map_err(|e| format!("{e}"))?,
            "--value-bytes" => {
                args.value_bytes = next("--value-bytes")?.parse().map_err(|e| format!("{e}"))?
            }
            "--batch-records" => {
                args.batch_records = next("--batch-records")?.parse().map_err(|e| format!("{e}"))?
            }
            "--inflight" => {
                args.inflight = next("--inflight")?.parse().map_err(|e| format!("{e}"))?
            }
            "--rate" => args.rate = next("--rate")?.parse().map_err(|e| format!("{e}"))?,
            "--acks" => {
                args.acks_label = next("--acks")?;
                args.acks = match args.acks_label.as_str() {
                    "none" => Acks::None,
                    "written" => Acks::Written,
                    "durable" => Acks::Durable,
                    other => return Err(format!("unknown acks {other}")),
                };
            }
            "--header" => args.header = true,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if !args.header && args.addr.is_empty() {
        return Err("required: --addr <host:port>".into());
    }
    if args.batch_records == 0 || args.inflight == 0 {
        return Err("--batch-records and --inflight must be positive".into());
    }
    Ok(args)
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

const HEADER: &str = "acks,batch_records,inflight,target_rate,records,value_bytes,\
elapsed_secs,records_per_sec,payload_mb_per_sec,p50_batch_us,p99_batch_us";

async fn await_ack(
    started: Instant,
    rx: tokio::sync::oneshot::Receiver<Response>,
    latencies_ns: &mut Vec<u64>,
) -> Result<(), String> {
    match rx.await {
        Ok(Response::Produce {
            error: ErrorCode::None,
            ..
        }) => {
            latencies_ns.push(started.elapsed().as_nanos() as u64);
            Ok(())
        }
        Ok(Response::Produce { error, .. }) => Err(format!("produce failed: {error:?}")),
        Ok(other) => Err(format!("unexpected response {other:?}")),
        Err(_) => Err("connection closed mid-bench".into()),
    }
}

async fn run(args: &Args) -> Result<String, String> {
    let conn = Connection::connect(&args.addr)
        .await
        .map_err(|e| e.to_string())?;
    match conn.create_topic("bench", 1).await {
        Ok(()) | Err(replog::client::ClientError::Broker(ErrorCode::TopicExists)) => {}
        Err(e) => return Err(e.to_string()),
    }

    let value: Vec<u8> = (0..args.value_bytes).map(|i| (i * 31) as u8).collect();
    let batches = args.records / args.batch_records;
    let records_sent = batches * args.batch_records;
    let make_request = || Request::Produce {
        topic: "bench".into(),
        partition: 0,
        acks: args.acks,
        records: (0..args.batch_records)
            .map(|_| ProduceRecord {
                key: None,
                value: value.clone(),
            })
            .collect(),
    };

    let mut latencies_ns: Vec<u64>;
    let start = Instant::now();
    let elapsed = if args.rate > 0 {
        let (elapsed, lat) = run_open_loop(&conn, args, batches, make_request, start).await?;
        latencies_ns = lat;
        elapsed
    } else {
        latencies_ns = Vec::with_capacity(batches);
        let mut inflight: VecDeque<(Instant, tokio::sync::oneshot::Receiver<Response>)> =
            VecDeque::new();
        for _ in 0..batches {
            let req = make_request();
            if args.acks == Acks::None {
                conn.send_only(&req).map_err(|e| e.to_string())?;
                continue;
            }
            let sent_at = Instant::now();
            let rx = conn.call_start(&req).map_err(|e| e.to_string())?;
            inflight.push_back((sent_at, rx));
            while inflight.len() >= args.inflight {
                let (t, rx) = inflight.pop_front().unwrap();
                await_ack(t, rx, &mut latencies_ns).await?;
            }
        }
        while let Some((t, rx)) = inflight.pop_front() {
            await_ack(t, rx, &mut latencies_ns).await?;
        }
        if args.acks == Acks::None {
            // Fence: an empty acked produce bounds when the broker has
            // processed everything sent before it on this connection.
            conn.produce("bench", 0, Acks::Written, Vec::new())
                .await
                .map_err(|e| e.to_string())?;
        }
        start.elapsed().as_secs_f64()
    };

    latencies_ns.sort_unstable();
    let payload_mb = (records_sent * args.value_bytes) as f64 / 1e6;
    Ok(format!(
        "{},{},{},{},{},{},{:.3},{:.0},{:.2},{:.1},{:.1}",
        args.acks_label,
        args.batch_records,
        args.inflight,
        args.rate,
        records_sent,
        args.value_bytes,
        elapsed,
        records_sent as f64 / elapsed,
        payload_mb / elapsed,
        percentile(&latencies_ns, 0.50) as f64 / 1e3,
        percentile(&latencies_ns, 0.99) as f64 / 1e3,
    ))
}

/// Open-loop load: batches are sent on a fixed schedule regardless of ack
/// progress; each ack task records latency relative to its scheduled send.
async fn run_open_loop(
    conn: &Connection,
    args: &Args,
    batches: usize,
    make_request: impl Fn() -> Request,
    start: Instant,
) -> Result<(f64, Vec<u64>), String> {
    use std::sync::{Arc, Mutex};
    if args.acks == Acks::None {
        return Err("--rate requires acked produces".into());
    }
    let interval =
        std::time::Duration::from_secs_f64(args.batch_records as f64 / args.rate as f64);
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(batches)));
    let failures = Arc::new(Mutex::new(0usize));
    let mut tasks = Vec::with_capacity(batches);
    for i in 0..batches {
        let scheduled = start + interval * i as u32;
        tokio::time::sleep_until(scheduled.into()).await;
        let rx = conn.call_start(&make_request()).map_err(|e| e.to_string())?;
        let latencies = latencies.clone();
        let failures = failures.clone();
        tasks.push(tokio::spawn(async move {
            match rx.await {
                Ok(Response::Produce {
                    error: ErrorCode::None,
                    ..
                }) => latencies
                    .lock()
                    .unwrap()
                    .push(scheduled.elapsed().as_nanos() as u64),
                _ => *failures.lock().unwrap() += 1,
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let failures = *failures.lock().unwrap();
    if failures > 0 {
        return Err(format!("{failures} batches failed"));
    }
    let latencies = Arc::try_unwrap(latencies).unwrap().into_inner().unwrap();
    Ok((elapsed, latencies))
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("broker_bench: {e}");
            return ExitCode::FAILURE;
        }
    };
    if args.header {
        println!("{HEADER}");
        return ExitCode::SUCCESS;
    }
    match run(&args).await {
        Ok(row) => {
            println!("{row}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("broker_bench: {e}");
            ExitCode::FAILURE
        }
    }
}
