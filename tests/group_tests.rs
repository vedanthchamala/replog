//! Stage 3 integration tests: partitioning, group membership, generation
//! fencing — and the stage eval: a consumer crash mid-stream triggers a
//! rebalance and the checker verifies at-least-once delivery across it.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use replog::broker::{Broker, BrokerConfig, BrokerHandle};
use replog::checker::History;
use replog::client::{Connection, GroupConsumer, TopicProducer, partition_for_key};
use replog::proto::{Acks, ErrorCode, Request, Response};
use replog::storage::{FsyncPolicy, LogConfig};

async fn start_broker(dir: &Path) -> BrokerHandle {
    Broker::start(
        "127.0.0.1:0",
        BrokerConfig::standalone(
            dir,
            LogConfig {
                fsync: FsyncPolicy::Batch {
                    max_bytes: 64 * 1024,
                    max_ms: 5,
                },
                ..LogConfig::default()
            },
        ),
    )
    .await
    .expect("broker start")
}

async fn join(
    conn: &Connection,
    group: &str,
    member_id: &str,
    session_timeout_ms: u32,
) -> (String, u64, Vec<(String, u32)>) {
    match conn
        .call(&Request::JoinGroup {
            group: group.into(),
            member_id: member_id.into(),
            session_timeout_ms,
            topics: vec!["t".into()],
        })
        .await
        .unwrap()
    {
        Response::JoinGroup {
            error: ErrorCode::None,
            member_id,
            generation,
            assignment,
        } => (member_id, generation, assignment),
        other => panic!("join failed: {other:?}"),
    }
}

async fn heartbeat(conn: &Connection, group: &str, member_id: &str, generation: u64) -> ErrorCode {
    match conn
        .call(&Request::Heartbeat {
            group: group.into(),
            member_id: member_id.into(),
            generation,
        })
        .await
        .unwrap()
    {
        Response::Heartbeat { error } => error,
        other => panic!("unexpected response {other:?}"),
    }
}

fn partitions_of(assignment: &[(String, u32)]) -> HashSet<u32> {
    assignment.iter().map(|(_, p)| *p).collect()
}

#[test]
fn keyed_partitioning_is_stable() {
    for partitions in [1u32, 2, 4, 16] {
        let mut hit = HashSet::new();
        for i in 0..1000 {
            let key = format!("key-{i}").into_bytes();
            let p = partition_for_key(&key, partitions);
            assert_eq!(p, partition_for_key(&key, partitions), "must be stable");
            assert!(p < partitions);
            hit.insert(p);
        }
        assert_eq!(
            hit.len(),
            partitions as usize,
            "1000 keys must hit all {partitions} partitions"
        );
    }
}

#[tokio::test]
async fn members_split_partitions_and_rebalance_on_membership_change() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();
    conn.create_topic("t", 4).await.unwrap();

    let (m1, gen1, a1) = join(&conn, "g", "", 30_000).await;
    assert_eq!(partitions_of(&a1), HashSet::from([0, 1, 2, 3]));

    let (m2, gen2, a2) = join(&conn, "g", "", 30_000).await;
    assert!(gen2 > gen1);
    assert_eq!(heartbeat(&conn, "g", &m1, gen1).await, ErrorCode::StaleGeneration);

    let (_, gen1b, a1b) = join(&conn, "g", &m1, 30_000).await;
    assert_eq!(gen1b, gen2);
    assert_eq!(a1b.len(), 2);
    assert_eq!(a2.len(), 2);
    let union: HashSet<u32> = partitions_of(&a1b).union(&partitions_of(&a2)).copied().collect();
    assert_eq!(union, HashSet::from([0, 1, 2, 3]));
    assert!(partitions_of(&a1b).is_disjoint(&partitions_of(&a2)));

    let (m3, gen3, a3) = join(&conn, "g", "", 30_000).await;
    assert!(gen3 > gen2);
    let (_, _, a1c) = join(&conn, "g", &m1, 30_000).await;
    let (_, _, a2c) = join(&conn, "g", &m2, 30_000).await;
    let mut sizes = vec![a1c.len(), a2c.len(), a3.len()];
    sizes.sort_unstable();
    assert_eq!(sizes, vec![1, 1, 2]);

    match conn
        .call(&Request::LeaveGroup {
            group: "g".into(),
            member_id: m3.clone(),
        })
        .await
        .unwrap()
    {
        Response::LeaveGroup {
            error: ErrorCode::None,
        } => {}
        other => panic!("leave failed: {other:?}"),
    }
    let (_, gen4, a1d) = join(&conn, "g", &m1, 30_000).await;
    assert!(gen4 > gen3);
    assert_eq!(a1d.len(), 2);

    broker.shutdown().await;
}

#[tokio::test]
async fn stale_generation_commit_is_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();
    conn.create_topic("t", 2).await.unwrap();

    let (m1, gen1, _) = join(&conn, "g", "", 30_000).await;
    let (_m2, _gen2, _) = join(&conn, "g", "", 30_000).await;

    // m1's generation is now stale; its commit must be rejected.
    let err = conn
        .commit_offset_fenced("g", "t", 0, 42, &m1, gen1)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            replog::client::ClientError::Broker(ErrorCode::StaleGeneration)
        ),
        "got {err:?}"
    );
    assert_eq!(conn.fetch_offset("g", "t", 0).await.unwrap(), None);

    // After rejoining at the current generation the commit goes through.
    let (_, gen_current, _) = join(&conn, "g", &m1, 30_000).await;
    conn.commit_offset_fenced("g", "t", 0, 42, &m1, gen_current)
        .await
        .unwrap();
    assert_eq!(conn.fetch_offset("g", "t", 0).await.unwrap(), Some(42));

    broker.shutdown().await;
}

#[tokio::test]
async fn silent_member_is_evicted_after_session_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();
    conn.create_topic("t", 4).await.unwrap();

    let (m1, _, _) = join(&conn, "g", "", 30_000).await;
    let (_m2, gen2, _) = join(&conn, "g", "", 500).await;
    let (_, _, a1) = join(&conn, "g", &m1, 30_000).await;
    assert_eq!(a1.len(), 2);

    // m2 goes silent; after its 500 ms session timeout (+ sweep) it is
    // evicted and m1's next heartbeat reports the rebalance.
    let evicted_within = Duration::from_secs(3);
    let start = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        match heartbeat(&conn, "g", &m1, gen2).await {
            ErrorCode::None => {
                assert!(
                    start.elapsed() < evicted_within,
                    "m2 was not evicted within {evicted_within:?}"
                );
            }
            ErrorCode::StaleGeneration => break,
            other => panic!("unexpected heartbeat error {other:?}"),
        }
    }
    let (_, _, a1b) = join(&conn, "g", &m1, 30_000).await;
    assert_eq!(partitions_of(&a1b), HashSet::from([0, 1, 2, 3]));

    broker.shutdown().await;
}

/// The Stage 3 eval: keyed workload, two group consumers, one crashes without
/// leaving; the checker verifies at-least-once delivery across the rebalance.
#[tokio::test]
async fn rebalance_preserves_at_least_once_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let addr = broker.addr.to_string();
    let conn = Connection::connect(&addr).await.unwrap();
    conn.create_topic("ev", 4).await.unwrap();

    let history = Arc::new(Mutex::new(History::new()));
    let stop = Arc::new(AtomicBool::new(false));
    const TOTAL: u64 = 3000;

    let producer_history = history.clone();
    let producer_addr = addr.clone();
    let producer_task = tokio::spawn(async move {
        let conn = Connection::connect(&producer_addr).await.unwrap();
        let mut producer = TopicProducer::new(conn, "ev", Acks::Written, 25).await.unwrap();
        let mut pending: Vec<Vec<u64>> = vec![Vec::new(); 4];
        for id in 0..TOTAL {
            let key = format!("k{}", id % 61).into_bytes();
            let expect = partition_for_key(&key, 4);
            pending[expect as usize].push(id);
            if let Some((p, _)) = producer
                .send(Some(key), id.to_le_bytes().to_vec())
                .await
                .unwrap()
            {
                assert_eq!(p, expect);
                let mut h = producer_history.lock().unwrap();
                for acked in pending[p as usize].drain(..) {
                    h.record_produced(acked, "ev");
                }
            }
            if id % 25 == 24 {
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        }
        producer.flush().await.unwrap();
        let mut h = producer_history.lock().unwrap();
        for partition_pending in &mut pending {
            for acked in partition_pending.drain(..) {
                h.record_produced(acked, "ev");
            }
        }
    });

    let record = |history: &Arc<Mutex<History>>,
                  name: &str,
                  generation: u64,
                  batch: &[(String, u32, replog::proto::FetchedRecord)]| {
        let mut h = history.lock().unwrap();
        for (topic, partition, r) in batch {
            let id = u64::from_le_bytes(r.value[..8].try_into().unwrap());
            h.record_consumed(name, generation, topic, *partition, r.offset, id);
        }
    };

    // Consumer B: consumes ~300 records, then crashes (no LeaveGroup).
    let b_history = history.clone();
    let b_addr = addr.clone();
    let b_task = tokio::spawn(async move {
        let conn = Connection::connect(&b_addr).await.unwrap();
        let mut b = GroupConsumer::join(conn, "g", vec!["ev".into()], 1000)
            .await
            .unwrap();
        b.max_wait_ms = 50;
        let mut count = 0usize;
        while count < 300 {
            let records = b.poll().await.unwrap();
            record(&b_history, "B", b.generation(), &records);
            count += records.len();
            if count >= 150 {
                let _ = b.commit().await;
            }
        }
        Instant::now()
    });

    // Consumer A: runs until every acked id is covered.
    let a_history = history.clone();
    let a_stop = stop.clone();
    let a_task = tokio::spawn(async move {
        let conn = Connection::connect(&addr).await.unwrap();
        let mut a = GroupConsumer::join(conn, "g", vec!["ev".into()], 1000)
            .await
            .unwrap();
        a.max_wait_ms = 50;
        let mut full_ownership_at = None;
        let mut polls = 0usize;
        while !a_stop.load(Ordering::Relaxed) {
            let records = a.poll().await.unwrap();
            record(&a_history, "A", a.generation(), &records);
            if full_ownership_at.is_none() && a.assignment().len() == 4 {
                full_ownership_at = Some(Instant::now());
            }
            polls += 1;
            if polls % 10 == 0 {
                let _ = a.commit().await;
            }
        }
        full_ownership_at
    });

    let b_crashed_at = b_task.await.unwrap();
    producer_task.await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        {
            let h = history.lock().unwrap();
            if h.produced_ids().is_subset(&h.consumed_ids()) {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "acked records not fully consumed within 30s of rebalance"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.store(true, Ordering::Relaxed);
    let full_ownership_at = a_task.await.unwrap();
    broker.shutdown().await;

    let report = history.lock().unwrap().verify();
    println!("{report}");
    if let Some(t) = full_ownership_at {
        println!(
            "rebalance takeover: A owned all 4 partitions {:?} after B crashed",
            t.duration_since(b_crashed_at)
        );
    }
    assert_eq!(report.produced_acked as u64, TOTAL);
    assert!(report.is_ok(), "{report}");
}
