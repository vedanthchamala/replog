//! Process-boundary harness: a real cluster of *child processes* (one
//! controller + N brokers) that can be killed with SIGKILL and restarted on
//! their data directories.
//!
//! This is deliberately library code, not test glue: the Stage 4
//! process-boundary failover eval, the failover-time bench, and the Stage 5
//! torture harness all drive clusters the same way — spawn the real binaries,
//! parse their "listening on" line, `kill -9` by id, restart on the same
//! data dir, and watch the controller's metadata to know when the cluster has
//! reacted. Killing a process (not aborting a task) is the whole point: no
//! in-process softening survives a SIGKILL.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::client::Connection;
use crate::proto::{ClusterMeta, ErrorCode, Request, Response};

/// Everything needed to spawn a cluster. Binary paths come from the caller:
/// integration tests use `env!("CARGO_BIN_EXE_...")`, bench bins locate their
/// siblings in the same target directory.
#[derive(Clone)]
pub struct ClusterSpec {
    pub controller_bin: PathBuf,
    pub broker_bin: PathBuf,
    /// Data root; the caller owns its lifetime (e.g. a TempDir).
    pub root: PathBuf,
    pub brokers: usize,
    pub fsync: String,
    pub min_isr: u32,
    pub replica_lag_ms: u64,
    pub session_timeout_ms: u64,
}

impl ClusterSpec {
    pub fn new(
        controller_bin: impl Into<PathBuf>,
        broker_bin: impl Into<PathBuf>,
        root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            controller_bin: controller_bin.into(),
            broker_bin: broker_bin.into(),
            root: root.into(),
            brokers: 3,
            fsync: "batch:1048576:5".into(),
            min_isr: 2,
            replica_lag_ms: 1500,
            session_timeout_ms: 700,
        }
    }
}

pub struct ProcBroker {
    pub id: u32,
    pub addr: String,
    child: Child,
}

impl Drop for ProcBroker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub struct ProcCluster {
    spec: ClusterSpec,
    pub controller_addr: String,
    controller: Child,
    /// `None` = currently killed (slot keeps broker ids stable for restart).
    pub brokers: Vec<Option<ProcBroker>>,
}

impl Drop for ProcCluster {
    fn drop(&mut self) {
        let _ = self.controller.kill();
        let _ = self.controller.wait();
        // ProcBroker's own Drop reaps each broker.
    }
}

/// Spawns a binary that prints exactly one `... listening on <addr>` line on
/// stdout; stderr is appended to `stderr_log` for post-mortems.
fn spawn_and_get_addr(mut cmd: Command, stderr_log: &PathBuf) -> std::io::Result<(Child, String)> {
    let stderr = File::options().create(true).append(true).open(stderr_log)?;
    let mut child = cmd.stdout(Stdio::piped()).stderr(stderr).spawn()?;
    let stdout = child.stdout.take().expect("stdout piped");
    let mut line = String::new();
    BufReader::new(stdout).read_line(&mut line)?;
    let addr = line.trim().rsplit(' ').next().unwrap_or("").to_string();
    if !line.contains("listening on") || addr.is_empty() {
        let _ = child.kill();
        let _ = child.wait();
        return Err(std::io::Error::other(format!(
            "process did not announce a listen address (got {line:?})"
        )));
    }
    Ok((child, addr))
}

impl ProcCluster {
    pub async fn start(spec: ClusterSpec) -> std::io::Result<ProcCluster> {
        std::fs::create_dir_all(&spec.root)?;
        let mut controller_cmd = Command::new(&spec.controller_bin);
        controller_cmd.args([
            "--listen",
            "127.0.0.1:0",
            "--state-file",
            spec.root.join("controller.state").to_str().unwrap(),
            "--session-timeout-ms",
            &spec.session_timeout_ms.to_string(),
        ]);
        let (controller, controller_addr) =
            spawn_and_get_addr(controller_cmd, &spec.root.join("controller.stderr.log"))?;

        let mut cluster = ProcCluster {
            controller_addr,
            controller,
            brokers: Vec::new(),
            spec,
        };
        for id in 0..cluster.spec.brokers as u32 {
            let broker = cluster.spawn_broker(id)?;
            cluster.brokers.push(Some(broker));
        }
        let n = cluster.spec.brokers;
        cluster
            .wait_for_meta("all brokers to register", |m| m.brokers.len() >= n)
            .await;
        Ok(cluster)
    }

    fn spawn_broker(&self, id: u32) -> std::io::Result<ProcBroker> {
        let data_dir = self.spec.root.join(format!("broker-{id}"));
        let mut cmd = Command::new(&self.spec.broker_bin);
        cmd.args([
            "--listen",
            "127.0.0.1:0",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--fsync",
            &self.spec.fsync,
            "--broker-id",
            &id.to_string(),
            "--controller-addr",
            &self.controller_addr,
            "--min-isr",
            &self.spec.min_isr.to_string(),
            "--replica-lag-ms",
            &self.spec.replica_lag_ms.to_string(),
        ]);
        let (child, addr) =
            spawn_and_get_addr(cmd, &self.spec.root.join(format!("broker-{id}.stderr.log")))?;
        Ok(ProcBroker { id, addr, child })
    }

    /// Addresses of currently-live brokers.
    pub fn broker_addrs(&self) -> Vec<String> {
        self.brokers
            .iter()
            .flatten()
            .map(|b| b.addr.clone())
            .collect()
    }

    pub async fn controller_meta(&self) -> ClusterMeta {
        let conn = Connection::connect(&self.controller_addr)
            .await
            .expect("connect to controller");
        match conn.call(&Request::ControllerMetadata).await {
            Ok(Response::ControllerMetadata {
                error: ErrorCode::None,
                cluster,
            }) => cluster,
            other => panic!("controller metadata failed: {other:?}"),
        }
    }

    /// Polls controller metadata until `predicate` holds (15 s deadline).
    pub async fn wait_for_meta(
        &self,
        what: &str,
        predicate: impl Fn(&ClusterMeta) -> bool,
    ) -> ClusterMeta {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let meta = self.controller_meta().await;
            if predicate(&meta) {
                return meta;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// SIGKILL — the real thing, no flush, no goodbye. The slot stays
    /// reserved so the broker can be restarted on its data dir.
    pub fn kill9(&mut self, id: usize) {
        let broker = self.brokers[id].take().expect("broker already dead");
        drop(broker); // Drop sends SIGKILL and reaps.
    }

    /// Restarts a killed broker on its old data dir (new port) and waits for
    /// the controller to see the re-registration.
    pub async fn restart(&mut self, id: usize) -> std::io::Result<()> {
        assert!(self.brokers[id].is_none(), "broker {id} is running");
        let broker = self.spawn_broker(id as u32)?;
        let addr = broker.addr.clone();
        self.brokers[id] = Some(broker);
        self.wait_for_meta("restarted broker to re-register", |m| {
            m.brokers.iter().any(|(bid, a)| *bid == id as u32 && *a == addr)
        })
        .await;
        Ok(())
    }
}

pub fn leader_of(meta: &ClusterMeta, topic: &str, partition: u32) -> i32 {
    meta.topics
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == partition))
        .map(|p| p.leader)
        .unwrap_or(-1)
}

pub fn isr_of(meta: &ClusterMeta, topic: &str, partition: u32) -> Vec<u32> {
    meta.topics
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == partition))
        .map(|p| p.isr.clone())
        .unwrap_or_default()
}
