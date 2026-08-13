//! The controller binary: `replog_controller --listen 127.0.0.1:9090
//! --state-file ./controller.state [--session-timeout-ms 3000]`

use std::io::Write;
use std::process::ExitCode;
use std::time::Duration;

use replog::controller::{Controller, ControllerConfig};

fn parse_args() -> Result<(String, ControllerConfig), String> {
    let mut listen = "127.0.0.1:9090".to_string();
    let mut state_file = None;
    let mut session_timeout_ms = 3000u64;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--listen" => listen = it.next().ok_or("--listen needs a value")?,
            "--state-file" => state_file = Some(it.next().ok_or("--state-file needs a value")?),
            "--session-timeout-ms" => {
                session_timeout_ms = it
                    .next()
                    .ok_or("--session-timeout-ms needs a value")?
                    .parse()
                    .map_err(|e| format!("{e}"))?
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let state_file = state_file.ok_or("required: --state-file <path>")?;
    Ok((
        listen,
        ControllerConfig {
            state_file: state_file.into(),
            session_timeout: Duration::from_millis(session_timeout_ms),
        },
    ))
}

#[tokio::main]
async fn main() -> ExitCode {
    let (listen, config) = match parse_args() {
        Ok(x) => x,
        Err(e) => {
            eprintln!("replog_controller: {e}");
            return ExitCode::FAILURE;
        }
    };
    let handle = match Controller::start(&listen, config).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("replog_controller: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("replog controller listening on {}", handle.addr);
    let _ = std::io::stdout().flush();
    std::future::pending::<()>().await;
    ExitCode::SUCCESS
}
