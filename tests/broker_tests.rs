//! Stage 2 integration tests: the broker's ack contract, long-poll fetch,
//! pipelining, offset resume — and the one that matters most, durability of
//! acked writes across `kill -9` of a real broker process.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use replog::broker::{Broker, BrokerConfig, BrokerHandle};
use replog::client::{Connection, Consumer, Producer};
use replog::proto::{Acks, ErrorCode, ProduceRecord};
use replog::storage::{FsyncPolicy, LogConfig};

fn test_config(dir: &Path) -> BrokerConfig {
    BrokerConfig::standalone(
        dir,
        LogConfig {
            fsync: FsyncPolicy::Batch {
                max_bytes: 64 * 1024,
                max_ms: 5,
            },
            ..LogConfig::default()
        },
    )
}

async fn start_broker(dir: &Path) -> BrokerHandle {
    Broker::start("127.0.0.1:0", test_config(dir))
        .await
        .expect("broker start")
}

#[tokio::test]
async fn produce_fetch_roundtrip_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();

    conn.create_topic("events", 2).await.unwrap();
    assert!(matches!(
        conn.create_topic("events", 2).await,
        Err(replog::client::ClientError::Broker(ErrorCode::TopicExists))
    ));
    let cluster = conn.metadata().await.unwrap();
    assert_eq!(cluster.topics.len(), 1);
    assert_eq!(cluster.topics[0].name, "events");
    assert_eq!(cluster.topics[0].partitions.len(), 2);

    let make_record = |i: usize| ProduceRecord {
        key: match i % 3 {
            0 => None,
            _ => Some(format!("key-{i}").into_bytes()),
        },
        value: match i % 50 {
            49 => vec![0xC3; 100 * 1024],
            _ => format!("value-{i}").into_bytes(),
        },
    };

    let total = 5000usize;
    for batch_start in (0..total).step_by(100) {
        let records: Vec<_> = (batch_start..batch_start + 100).map(make_record).collect();
        let base = conn
            .produce("events", 0, Acks::Written, records)
            .await
            .unwrap();
        assert_eq!(base, Some(batch_start as u64));
    }
    conn.produce(
        "events",
        1,
        Acks::Written,
        vec![ProduceRecord {
            key: Some(b"other-partition".to_vec()),
            value: b"isolated".to_vec(),
        }],
    )
    .await
    .unwrap();

    let mut position = 0u64;
    let mut fetched = Vec::new();
    loop {
        let (log_start, next_offset, records) =
            conn.fetch("events", 0, position, 1 << 20, 0).await.unwrap();
        assert_eq!(log_start, 0);
        if records.is_empty() {
            assert_eq!(next_offset, total as u64);
            break;
        }
        for r in records {
            assert_eq!(r.offset, position, "offsets must be contiguous");
            position += 1;
            fetched.push(r);
        }
    }
    assert_eq!(fetched.len(), total);
    for (i, r) in fetched.iter().enumerate() {
        let expected = make_record(i);
        assert_eq!(r.key, expected.key, "key mismatch at {i}");
        assert_eq!(r.value, expected.value, "value mismatch at {i}");
    }

    let (_, _, other) = conn.fetch("events", 1, 0, 1 << 20, 0).await.unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].value, b"isolated");

    broker.shutdown().await;
}

#[tokio::test]
async fn errors_for_unknown_topic_and_bad_offset() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();

    assert!(matches!(
        conn.produce("nope", 0, Acks::Written, vec![]).await,
        Err(replog::client::ClientError::Broker(
            ErrorCode::UnknownTopicOrPartition
        ))
    ));
    conn.create_topic("t", 1).await.unwrap();
    assert!(matches!(
        conn.fetch("t", 0, 99, 1 << 20, 0).await,
        Err(replog::client::ClientError::Broker(ErrorCode::OffsetOutOfRange))
    ));

    broker.shutdown().await;
}

#[tokio::test]
async fn long_poll_fetch_completes_on_produce() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();
    conn.create_topic("t", 1).await.unwrap();

    let waiter = conn.clone();
    let started = Instant::now();
    let parked = tokio::spawn(async move { waiter.fetch("t", 0, 0, 1 << 20, 5000).await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    conn.produce(
        "t",
        0,
        Acks::Written,
        vec![ProduceRecord {
            key: None,
            value: b"wakeup".to_vec(),
        }],
    )
    .await
    .unwrap();

    let (_, next_offset, records) = parked.await.unwrap().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value, b"wakeup");
    assert_eq!(next_offset, 1);
    assert!(
        started.elapsed() < Duration::from_millis(4000),
        "long-poll must return on produce, not at the timeout"
    );

    broker.shutdown().await;
}

#[tokio::test]
async fn pipelined_produces_stay_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();
    conn.create_topic("t", 1).await.unwrap();

    let mut replies = Vec::new();
    for i in 0..50u64 {
        let req = replog::proto::Request::Produce {
            topic: "t".into(),
            partition: 0,
            acks: Acks::Written,
            leader_epoch: 0,
            records: vec![ProduceRecord {
                key: None,
                value: i.to_le_bytes().to_vec(),
            }],
        };
        replies.push(conn.call_start(&req).unwrap());
    }
    for (i, rx) in replies.into_iter().enumerate() {
        match rx.await.unwrap() {
            replog::proto::Response::Produce {
                error: ErrorCode::None,
                base_offset,
                count,
            } => {
                assert_eq!(base_offset, i as u64, "pipelined produce order broken");
                assert_eq!(count, 1);
            }
            other => panic!("unexpected response {other:?}"),
        }
    }

    broker.shutdown().await;
}

#[tokio::test]
async fn consumer_resumes_from_committed_offset_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();
    conn.create_topic("t", 1).await.unwrap();

    let mut producer = Producer::new(conn.clone(), "t", 0, Acks::Written, 20);
    for i in 0..100u64 {
        producer.send(None, format!("v{i}").into_bytes()).await.unwrap();
    }
    producer.flush().await.unwrap();

    conn.commit_offset("group-a", "t", 0, 60).await.unwrap();
    assert_eq!(conn.fetch_offset("group-a", "t", 0).await.unwrap(), Some(60));
    assert_eq!(conn.fetch_offset("group-b", "t", 0).await.unwrap(), None);
    broker.shutdown().await;

    let broker = start_broker(dir.path()).await;
    let conn = Connection::connect(&broker.addr.to_string()).await.unwrap();
    let mut consumer = Consumer::resume(conn, "t", 0, "group-a").await.unwrap();
    assert_eq!(consumer.position(), 60, "must resume from the committed offset");

    let mut seen = Vec::new();
    while seen.len() < 40 {
        let records = consumer.poll().await.unwrap();
        assert!(!records.is_empty(), "expected the remaining 40 records");
        seen.extend(records);
    }
    assert_eq!(seen.first().unwrap().offset, 60);
    assert_eq!(seen.last().unwrap().offset, 99);
    consumer.commit().await.unwrap();

    broker.shutdown().await;
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_broker_process(data_dir: &Path, fsync: &str) -> (ChildGuard, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_replog_broker"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--fsync",
            fsync,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn broker process");
    let stdout = child.stdout.take().unwrap();
    let mut line = String::new();
    BufReader::new(stdout).read_line(&mut line).expect("read listen line");
    let addr = line
        .trim()
        .rsplit(' ')
        .next()
        .expect("addr in listen line")
        .to_string();
    (ChildGuard(child), addr)
}

/// The Stage 2 durability contract: an acks=durable produce acked by a real
/// broker process survives `kill -9` of that process.
#[tokio::test]
async fn durable_acked_records_survive_kill_dash_nine() {
    let dir = tempfile::tempdir().unwrap();
    let (mut child, addr) = spawn_broker_process(dir.path(), "batch:4096:2");

    let conn = Connection::connect(&addr).await.unwrap();
    conn.create_topic("crash", 1).await.unwrap();
    let total = 50u64;
    for i in 0..total {
        let base = conn
            .produce(
                "crash",
                0,
                Acks::Durable,
                vec![ProduceRecord {
                    key: Some(i.to_le_bytes().to_vec()),
                    value: format!("durable-{i}").into_bytes(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(base, Some(i));
    }

    child.0.kill().expect("SIGKILL broker");
    child.0.wait().expect("reap broker");

    let (_child2, addr2) = spawn_broker_process(dir.path(), "batch:4096:2");
    let conn = Connection::connect(&addr2).await.unwrap();
    let (log_start, next_offset, records) =
        conn.fetch("crash", 0, 0, 10 << 20, 0).await.unwrap();
    assert_eq!(log_start, 0);
    assert_eq!(
        next_offset, total,
        "every durable-acked record must survive kill -9"
    );
    assert_eq!(records.len(), total as usize);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.offset, i as u64);
        assert_eq!(r.value, format!("durable-{i}").into_bytes());
    }
}
