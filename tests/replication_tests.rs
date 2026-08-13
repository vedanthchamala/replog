//! Stage 4 evals: replication convergence, HWM/min-ISR enforcement, failover
//! with zero acked loss (checker-verified), and stale-leader truncation.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use replog::broker::{Broker, BrokerConfig, BrokerHandle};
use replog::client::{ClientError, ClusterClient, Connection};
use replog::controller::{Controller, ControllerConfig, ControllerHandle};
use replog::proto::{Acks, ClusterMeta, ErrorCode, ProduceRecord, Request, Response};
use replog::storage::{FsyncPolicy, Log, LogConfig};

fn log_config() -> LogConfig {
    LogConfig {
        fsync: FsyncPolicy::Batch {
            max_bytes: 256 * 1024,
            max_ms: 5,
        },
        ..LogConfig::default()
    }
}

struct Cluster {
    controller: ControllerHandle,
    brokers: Vec<Option<BrokerHandle>>,
    dirs: Vec<PathBuf>,
    _root: tempfile::TempDir,
}

impl Cluster {
    async fn start(n: usize, min_isr: u32, session_timeout_ms: u64, lag_ms: u64) -> Cluster {
        let root = tempfile::tempdir().unwrap();
        let controller = Controller::start(
            "127.0.0.1:0",
            ControllerConfig {
                state_file: root.path().join("controller.state"),
                session_timeout: Duration::from_millis(session_timeout_ms),
            },
        )
        .await
        .unwrap();
        let controller_addr = controller.addr.to_string();
        let mut brokers = Vec::new();
        let mut dirs = Vec::new();
        for id in 0..n {
            let dir = root.path().join(format!("broker-{id}"));
            let mut config =
                BrokerConfig::clustered(&dir, log_config(), id as u32, &controller_addr);
            config.min_insync_replicas = min_isr;
            config.replica_lag_ms = lag_ms;
            brokers.push(Some(Broker::start("127.0.0.1:0", config).await.unwrap()));
            dirs.push(dir);
        }
        let cluster = Cluster {
            controller,
            brokers,
            dirs,
            _root: root,
        };
        cluster.wait_for_brokers(n).await;
        cluster
    }

    fn broker_addrs(&self) -> Vec<String> {
        self.brokers
            .iter()
            .flatten()
            .map(|b| b.addr.to_string())
            .collect()
    }

    async fn controller_meta(&self) -> ClusterMeta {
        let conn = Connection::connect(&self.controller.addr.to_string())
            .await
            .unwrap();
        match conn.call(&Request::ControllerMetadata).await.unwrap() {
            Response::ControllerMetadata {
                error: ErrorCode::None,
                cluster,
            } => cluster,
            other => panic!("metadata failed: {other:?}"),
        }
    }

    async fn wait_for_brokers(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self.controller_meta().await.brokers.len() >= n {
                return;
            }
            assert!(Instant::now() < deadline, "brokers did not register");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Waits until `predicate` holds on controller metadata.
    async fn wait_for_meta(&self, what: &str, predicate: impl Fn(&ClusterMeta) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let meta = self.controller_meta().await;
            if predicate(&meta) {
                return;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn leader_of(meta: &ClusterMeta, topic: &str, partition: u32) -> i32 {
        meta.topics
            .iter()
            .find(|t| t.name == topic)
            .and_then(|t| t.partitions.iter().find(|p| p.partition == partition))
            .map(|p| p.leader)
            .unwrap_or(-1)
    }

    /// Returns the tempdir so callers that read the logs back from disk can
    /// keep it alive; dropping the Cluster would delete the data first.
    async fn shutdown(mut self) -> tempfile::TempDir {
        for b in self.brokers.iter_mut() {
            if let Some(handle) = b.take() {
                handle.shutdown().await;
            }
        }
        self.controller.shutdown().await;
        self._root
    }
}

fn read_all(dir: &Path) -> Vec<(u64, Option<Vec<u8>>, Vec<u8>)> {
    let log = Log::open(dir, log_config()).unwrap();
    let mut out = Vec::new();
    let mut offset = log.start_offset();
    while offset < log.next_offset() {
        for r in log.read(offset, 1 << 20).unwrap() {
            offset = r.offset + 1;
            out.push((r.offset, r.key, r.value));
        }
    }
    out
}

#[tokio::test]
async fn replicated_logs_converge_and_acks_all_works() {
    let cluster = Cluster::start(3, 2, 1000, 1500).await;
    let addrs = cluster.broker_addrs();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let client = ClusterClient::connect(&addr_refs).await.unwrap();
    client.create_topic("r", 1, 3).await.unwrap();
    cluster
        .wait_for_meta("leader elected", |m| Cluster::leader_of(m, "r", 0) >= 0)
        .await;
    // Let all three replicas join the ISR before demanding acks=all.
    cluster
        .wait_for_meta("full ISR", |m| {
            m.topics
                .iter()
                .find(|t| t.name == "r")
                .is_some_and(|t| t.partitions[0].isr.len() == 3)
        })
        .await;

    let total = 500u64;
    for base in (0..total).step_by(50) {
        let records = (base..base + 50)
            .map(|i| ProduceRecord {
                key: Some(i.to_le_bytes().to_vec()),
                value: format!("value-{i}").into_bytes(),
            })
            .collect();
        let acked = client.produce("r", 0, Acks::All, records).await.unwrap();
        assert_eq!(acked, Some(base));
    }

    let mut position = 0u64;
    let mut seen = Vec::new();
    while position < total {
        let (_, hwm, records) = client.fetch("r", 0, position, 1 << 20, 1000).await.unwrap();
        assert!(hwm <= total);
        for r in records {
            assert_eq!(r.offset, position);
            position += 1;
            seen.push(r);
        }
    }
    assert_eq!(seen.len(), total as usize);

    let dirs = cluster.dirs.clone();
    let _data = cluster.shutdown().await;
    let logs: Vec<_> = dirs.iter().map(|d| read_all(&d.join("r-0"))).collect();
    assert_eq!(logs[0].len(), total as usize);
    assert_eq!(logs[0], logs[1], "replica 1 diverges from leader");
    assert_eq!(logs[0], logs[2], "replica 2 diverges from leader");
}

#[tokio::test]
async fn acks_all_refused_when_isr_below_min() {
    let cluster = Cluster::start(3, 2, 700, 700).await;
    let addrs = cluster.broker_addrs();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let client = ClusterClient::connect(&addr_refs).await.unwrap();
    client.create_topic("m", 1, 3).await.unwrap();
    cluster
        .wait_for_meta("full ISR", |m| {
            m.topics
                .iter()
                .find(|t| t.name == "m")
                .is_some_and(|t| t.partitions[0].isr.len() == 3)
        })
        .await;

    let record = || {
        vec![ProduceRecord {
            key: None,
            value: b"x".to_vec(),
        }]
    };
    client.produce("m", 0, Acks::All, record()).await.unwrap();

    // Kill both non-leaders; the ISR must shrink to the leader alone and
    // acks=all must start being refused (durability cannot be pretended).
    let meta = cluster.controller_meta().await;
    let leader = Cluster::leader_of(&meta, "m", 0) as usize;
    let mut cluster = cluster;
    for id in 0..3 {
        if id != leader {
            cluster.brokers[id].take().unwrap().abort();
        }
    }
    cluster
        .wait_for_meta("ISR shrink", |m| {
            m.topics
                .iter()
                .find(|t| t.name == "m")
                .is_some_and(|t| t.partitions[0].isr.len() == 1)
        })
        .await;

    let leader_conn = Connection::connect(&cluster.brokers[leader].as_ref().unwrap().addr.to_string())
        .await
        .unwrap();
    let err = leader_conn
        .produce("m", 0, Acks::All, record())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ClientError::Broker(ErrorCode::NotEnoughReplicas)),
        "got {err:?}"
    );
    // Weaker ack levels still work: availability is the client's choice.
    leader_conn
        .produce("m", 0, Acks::Written, record())
        .await
        .unwrap();

    cluster.shutdown().await;
}

/// The SPEC pass condition, in-process form: kill the leader mid-stream at
/// acks=all; a new leader is elected; zero acked records are lost.
#[tokio::test]
async fn failover_loses_no_acked_records() {
    let cluster = Cluster::start(3, 2, 700, 1000).await;
    let addrs = cluster.broker_addrs();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let client = ClusterClient::connect(&addr_refs).await.unwrap();
    client.create_topic("f", 1, 3).await.unwrap();
    cluster
        .wait_for_meta("full ISR", |m| {
            m.topics
                .iter()
                .find(|t| t.name == "f")
                .is_some_and(|t| t.partitions[0].isr.len() == 3)
        })
        .await;

    let meta = cluster.controller_meta().await;
    let original_leader = Cluster::leader_of(&meta, "f", 0);
    assert!(original_leader >= 0);

    let mut cluster = cluster;
    let mut acked: Vec<u64> = Vec::new();
    let mut killed_at: Option<Instant> = None;
    let mut first_ack_after_kill: Option<Duration> = None;
    let total = 1200u64;
    for base in (0..total).step_by(20) {
        if base == 400 {
            // Mid-stream: kill the leader without ceremony.
            cluster.brokers[original_leader as usize].take().unwrap().abort();
            killed_at = Some(Instant::now());
        }
        let records: Vec<ProduceRecord> = (base..base + 20)
            .map(|i| ProduceRecord {
                key: None,
                value: i.to_le_bytes().to_vec(),
            })
            .collect();
        match client.produce("f", 0, Acks::All, records).await {
            Ok(_) => {
                if let (Some(t), None) = (killed_at, first_ack_after_kill) {
                    first_ack_after_kill = Some(t.elapsed());
                }
                acked.extend(base..base + 20);
            }
            Err(e) => {
                // A failed batch is simply not acked; the contract says
                // nothing about it. (Retries inside ClusterClient already
                // absorbed transient leadership errors.)
                eprintln!("batch at {base} not acked: {e}");
            }
        }
    }
    assert!(
        acked.len() >= 1000,
        "workload should mostly succeed through the failover ({} acked)",
        acked.len()
    );

    let new_meta = cluster.controller_meta().await;
    let new_leader = Cluster::leader_of(&new_meta, "f", 0);
    assert!(new_leader >= 0 && new_leader != original_leader, "new leader elected");

    // Read everything back through the new leader and verify every acked
    // value is present — the durability contract, checked from the outside.
    let mut consumed = std::collections::HashSet::new();
    let mut position = 0u64;
    loop {
        let (_, hwm, records) = client.fetch("f", 0, position, 1 << 20, 500).await.unwrap();
        if records.is_empty() {
            if position >= hwm {
                break;
            }
            continue;
        }
        for r in records {
            position = r.offset + 1;
            consumed.insert(u64::from_le_bytes(r.value[..8].try_into().unwrap()));
        }
    }
    let missing: Vec<u64> = acked
        .iter()
        .copied()
        .filter(|id| !consumed.contains(id))
        .collect();
    println!(
        "failover eval: {} acked, {} consumed ids, {} missing; first ack after kill: {:?}",
        acked.len(),
        consumed.len(),
        missing.len(),
        first_ack_after_kill,
    );
    assert!(missing.is_empty(), "ACKED RECORDS LOST: {missing:?}");

    cluster.shutdown().await;
}

/// Stale-leader rejoin: acks=written records that only the dead leader had
/// are truncated away when it returns as a follower; acks=all history and the
/// new leader's records win. Divergence handled by epochs, not hope.
#[tokio::test]
async fn stale_leader_rejoin_truncates_divergent_suffix() {
    let cluster = Cluster::start(2, 2, 700, 3000).await;
    let addrs = cluster.broker_addrs();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let client = ClusterClient::connect(&addr_refs).await.unwrap();
    client.create_topic("s", 1, 2).await.unwrap();
    cluster
        .wait_for_meta("full ISR", |m| {
            m.topics
                .iter()
                .find(|t| t.name == "s")
                .is_some_and(|t| t.partitions[0].isr.len() == 2)
        })
        .await;

    let meta = cluster.controller_meta().await;
    let leader_a = Cluster::leader_of(&meta, "s", 0);
    let follower_b = 1 - leader_a;

    // 100 fully replicated records.
    let records: Vec<ProduceRecord> = (0..100u64)
        .map(|i| ProduceRecord {
            key: None,
            value: format!("committed-{i}").into_bytes(),
        })
        .collect();
    client.produce("s", 0, Acks::All, records).await.unwrap();

    // Kill the follower, then write 30 acks=written records that exist only
    // on A (replica_lag_ms is high, so the ISR still lists B — these records
    // can never become HWM-committed, which is the point).
    let mut cluster = cluster;
    cluster.brokers[follower_b as usize].take().unwrap().abort();
    let conn_a = Connection::connect(&cluster.brokers[leader_a as usize].as_ref().unwrap().addr.to_string())
        .await
        .unwrap();
    for i in 0..30u64 {
        conn_a
            .produce(
                "s",
                0,
                Acks::Written,
                vec![ProduceRecord {
                    key: None,
                    value: format!("leader-only-{i}").into_bytes(),
                }],
            )
            .await
            .unwrap();
    }
    // Now kill A too, and bring B back: B is still in the recorded ISR, so
    // the controller elects it (epoch bump). A's 30 records are divergent.
    let dir_a = cluster.dirs[leader_a as usize].clone();
    let dir_b = cluster.dirs[follower_b as usize].clone();
    cluster.brokers[leader_a as usize].take().unwrap().abort();
    let controller_addr = cluster.controller.addr.to_string();
    let mut config_b =
        BrokerConfig::clustered(&dir_b, log_config(), follower_b as u32, &controller_addr);
    config_b.min_insync_replicas = 1;
    config_b.replica_lag_ms = 3000;
    cluster.brokers[follower_b as usize] =
        Some(Broker::start("127.0.0.1:0", config_b).await.unwrap());
    cluster
        .wait_for_meta("B elected", |m| Cluster::leader_of(m, "s", 0) == follower_b)
        .await;

    // B (leader, new epoch) takes 40 new records at acks=all with ISR={B}.
    // min_isr=1 on B permits it.
    let addr_b = cluster.brokers[follower_b as usize].as_ref().unwrap().addr.to_string();
    let conn_b = Connection::connect(&addr_b).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let result = conn_b
            .produce(
                "s",
                0,
                Acks::All,
                (0..40u64)
                    .map(|i| ProduceRecord {
                        key: None,
                        value: format!("new-epoch-{i}").into_bytes(),
                    })
                    .collect(),
            )
            .await;
        match result {
            Ok(_) => break,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("B never accepted produce: {e}"),
        }
    }

    // Restart A: it must come back as follower, truncate its 30 divergent
    // records via EpochCheck, and converge on B's history.
    let mut config_a =
        BrokerConfig::clustered(&dir_a, log_config(), leader_a as u32, &controller_addr);
    config_a.min_insync_replicas = 1;
    cluster.brokers[leader_a as usize] =
        Some(Broker::start("127.0.0.1:0", config_a).await.unwrap());

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let a_log_matches = {
            // Compare through B's HWM view: both logs must reach 140.
            let (_, hwm, _) = conn_b.fetch("s", 0, 0, 1, 0).await.unwrap();
            hwm == 140
        };
        if a_log_matches {
            // Check ISR contains A again = A caught up cleanly.
            let meta = cluster.controller_meta().await;
            let isr = meta
                .topics
                .iter()
                .find(|t| t.name == "s")
                .map(|t| t.partitions[0].isr.clone())
                .unwrap_or_default();
            if isr.len() == 2 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "A never converged/rejoined ISR");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let dirs = cluster.dirs.clone();
    let _data = cluster.shutdown().await;
    let log_a = read_all(&dirs[leader_a as usize].join("s-0"));
    let log_b = read_all(&dirs[follower_b as usize].join("s-0"));
    assert_eq!(log_a.len(), 140, "A must have exactly 100 + 40 records");
    assert_eq!(log_a, log_b, "logs must converge byte-identically");
    assert!(
        log_a.iter().all(|(_, _, v)| !v.starts_with(b"leader-only-")),
        "divergent acks=written records must be truncated away"
    );
}
