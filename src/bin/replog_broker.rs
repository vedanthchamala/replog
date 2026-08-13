//! The broker binary: `replog_broker --listen 127.0.0.1:9092 --data-dir ./data
//! [--fsync always|os|batch:<bytes>:<ms>]`
//!
//! Prints `replog broker listening on <addr>` once ready (tests parse it, and
//! `--listen 127.0.0.1:0` picks a free port).

use std::io::Write;
use std::process::ExitCode;

use replog::broker::{Broker, BrokerConfig};
use replog::storage::{FsyncPolicy, LogConfig};

fn parse_args() -> Result<(String, BrokerConfig), String> {
    let mut listen = "127.0.0.1:9092".to_string();
    let mut data_dir = None;
    let mut fsync = FsyncPolicy::Batch {
        max_bytes: 1024 * 1024,
        max_ms: 50,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--listen" => listen = it.next().ok_or("--listen needs a value")?,
            "--data-dir" => data_dir = Some(it.next().ok_or("--data-dir needs a value")?),
            "--fsync" => fsync = FsyncPolicy::parse(&it.next().ok_or("--fsync needs a value")?)?,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let data_dir = data_dir.ok_or("required: --data-dir <path>")?;
    Ok((
        listen,
        BrokerConfig {
            data_dir: data_dir.into(),
            log: LogConfig {
                fsync,
                ..LogConfig::default()
            },
        },
    ))
}

#[tokio::main]
async fn main() -> ExitCode {
    let (listen, config) = match parse_args() {
        Ok(x) => x,
        Err(e) => {
            eprintln!("replog_broker: {e}");
            return ExitCode::FAILURE;
        }
    };
    let handle = match Broker::start(&listen, config).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("replog_broker: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("replog broker listening on {}", handle.addr);
    let _ = std::io::stdout().flush();
    std::future::pending::<()>().await;
    ExitCode::SUCCESS
}
