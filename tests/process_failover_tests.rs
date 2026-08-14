//! The SPEC Stage 4 pass condition at the process boundary: `kill -9` the
//! partition leader — a real broker *process*, real SIGKILL — mid-stream at
//! acks=all; a new leader is elected; the checker verifies zero acked records
//! were lost. This removes the two softenings of the in-process eval (the
//! aborted broker's writer thread still flushed on its way out, and detached
//! tasks briefly outlived the "kill").

use replog::checker::History;
use replog::client::ClusterClient;
use replog::harness::{isr_of, leader_of, ClusterSpec, ProcCluster};
use replog::proto::{Acks, ProduceRecord};

#[tokio::test]
async fn process_failover_loses_no_acked_records() {
    let root = tempfile::tempdir().unwrap();
    let mut spec = ClusterSpec::new(
        env!("CARGO_BIN_EXE_replog_controller"),
        env!("CARGO_BIN_EXE_replog_broker"),
        root.path().join("cluster"),
    );
    spec.session_timeout_ms = 700;
    spec.replica_lag_ms = 1500;
    spec.fsync = "batch:262144:5".into();
    let mut cluster = ProcCluster::start(spec).await.unwrap();

    let addrs = cluster.broker_addrs();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let client = ClusterClient::connect(&addr_refs).await.unwrap();
    client.create_topic("f", 1, 3).await.unwrap();
    cluster
        .wait_for_meta("full ISR", |m| isr_of(m, "f", 0).len() == 3)
        .await;
    let meta = cluster.controller_meta().await;
    let original_leader = leader_of(&meta, "f", 0);
    assert!(original_leader >= 0);

    let mut history = History::new();
    let total = 1200u64;
    for base in (0..total).step_by(20) {
        if base == 400 {
            // Mid-stream: SIGKILL the leader process. No flush, no goodbye.
            cluster.kill9(original_leader as usize);
        }
        let records: Vec<ProduceRecord> = (base..base + 20)
            .map(|i| ProduceRecord {
                key: None,
                value: i.to_le_bytes().to_vec(),
            })
            .collect();
        match client.produce("f", 0, Acks::All, records).await {
            Ok(_) => {
                for id in base..base + 20 {
                    history.record_produced(id, "f");
                }
            }
            Err(e) => eprintln!("batch at {base} not acked: {e}"),
        }
    }
    let acked = history.produced_ids().len();
    assert!(
        acked >= 1000,
        "workload should mostly succeed through the failover ({acked} acked)"
    );

    let meta = cluster.controller_meta().await;
    let new_leader = leader_of(&meta, "f", 0);
    assert!(
        new_leader >= 0 && new_leader != original_leader,
        "a different broker must lead after the kill"
    );

    // Read everything back through the surviving cluster; the checker — not
    // an assertion inside the system — decides whether the contract held.
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
            let id = u64::from_le_bytes(r.value[..8].try_into().unwrap());
            history.record_consumed("reader", 0, "f", 0, r.offset, id);
        }
    }
    let report = history.verify();
    println!("process failover eval: {report}");
    assert!(
        report.missing_ids.is_empty(),
        "ACKED RECORDS LOST ACROSS kill -9: {:?}",
        report.missing_ids
    );
    assert!(report.is_ok(), "{report}");
}
