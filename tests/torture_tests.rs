//! Stage 5 harness integration: the control-plane partition (proxy cut)
//! produces the zombie-leader scenario with real processes — the controller
//! deposes the unreachable leader while it keeps serving clients and
//! accepting acks=1 divergence; on heal it must rejoin as follower and
//! truncate. This is the process-boundary form of the Stage 4 in-process
//! stale-leader eval.

use replog::client::{ClusterClient, Connection};
use replog::harness::proxy::TcpProxy;
use replog::harness::{isr_of, leader_of, ClusterSpec, ProcCluster};
use replog::proto::{Acks, ProduceRecord};

#[tokio::test]
async fn cut_controller_link_deposes_leader_and_heals_divergence() {
    let root = tempfile::tempdir().unwrap();
    let mut spec = ClusterSpec::new(
        env!("CARGO_BIN_EXE_replog_controller"),
        env!("CARGO_BIN_EXE_replog_broker"),
        root.path().join("cluster"),
    );
    spec.session_timeout_ms = 700;
    spec.replica_lag_ms = 1500;
    let mut cluster = ProcCluster::start_controller_only(spec).unwrap();
    let mut proxies = Vec::new();
    for id in 0..3u32 {
        let proxy = TcpProxy::start(cluster.controller_addr.clone()).await.unwrap();
        cluster.add_broker(id, Some(&proxy.addr.clone())).unwrap();
        proxies.push(proxy);
    }
    cluster
        .wait_for_meta("brokers registered", |m| m.brokers.len() >= 3)
        .await;

    let addrs = cluster.broker_addrs();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let client = ClusterClient::connect(&addr_refs).await.unwrap();
    client.create_topic("z", 1, 3).await.unwrap();
    cluster
        .wait_for_meta("full ISR", |m| isr_of(m, "z", 0).len() == 3)
        .await;
    let meta = cluster.controller_meta().await;
    let old_leader = leader_of(&meta, "z", 0);
    assert!(old_leader >= 0);

    let records: Vec<ProduceRecord> = (0..100u64)
        .map(|i| ProduceRecord {
            key: None,
            value: format!("committed-{i}").into_bytes(),
        })
        .collect();
    client.produce("z", 0, Acks::All, records).await.unwrap();

    // Sever the leader's controller link only: liveness lost, data path up.
    proxies[old_leader as usize].cut();
    cluster
        .wait_for_meta("re-election away from the zombie", |m| {
            let l = leader_of(m, "z", 0);
            l >= 0 && l != old_leader
        })
        .await;

    // The controller electing is not enough: the *followers* keep fetching
    // from the zombie until their next heartbeat delivers the new epoch, and
    // anything the zombie accepts in that window gets replicated (first
    // version of this test proved it: 20 "divergent" records became
    // committed history). Wait until both healthy brokers' own metadata
    // views show the new leader — their fetchers have switched — so writes
    // to the zombie can no longer propagate.
    let meta = cluster.controller_meta().await;
    let new_leader = leader_of(&meta, "z", 0);
    for (id, broker) in cluster.brokers.iter().flatten().map(|b| (b.id as i32, b)) {
        if id == old_leader {
            continue;
        }
        let conn = Connection::connect(&broker.addr).await.unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(view) = conn.metadata().await
                && leader_of(&view, "z", 0) == new_leader
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "broker {id} never learned the new epoch"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    // The zombie still serves clients — and still believes it leads: it
    // accepts acks=1 writes that can never become committed history.
    let zombie_addr = cluster.brokers[old_leader as usize].as_ref().unwrap().addr.clone();
    let zombie = Connection::connect(&zombie_addr).await.unwrap();
    for i in 0..20u64 {
        zombie
            .produce(
                "z",
                0,
                Acks::Written,
                vec![ProduceRecord {
                    key: None,
                    value: format!("zombie-{i}").into_bytes(),
                }],
            )
            .await
            .expect("zombie must still accept acks=1 (that is the scenario)");
    }

    // Meanwhile the healthy majority keeps taking acks=all writes.
    let _ = client.refresh_metadata().await;
    let records: Vec<ProduceRecord> = (0..40u64)
        .map(|i| ProduceRecord {
            key: None,
            value: format!("new-epoch-{i}").into_bytes(),
        })
        .collect();
    client.produce("z", 0, Acks::All, records).await.unwrap();

    // Heal: the deposed leader re-registers, becomes follower, truncates its
    // 20 divergent records via EpochCheck, catches up, and rejoins the ISR.
    proxies[old_leader as usize].heal();
    cluster
        .wait_for_meta("zombie rejoins the ISR as follower", |m| {
            isr_of(m, "z", 0).len() == 3
        })
        .await;

    // Read everything through the cluster: exactly the committed history,
    // none of the zombie's divergent suffix.
    let mut position = 0u64;
    let mut values = Vec::new();
    loop {
        let (_, hwm, records) = client.fetch("z", 0, position, 1 << 20, 500).await.unwrap();
        if records.is_empty() {
            if position >= hwm {
                break;
            }
            continue;
        }
        for r in records {
            position = r.offset + 1;
            values.push(r.value);
        }
    }
    assert_eq!(values.len(), 140, "100 committed + 40 new-epoch records");
    assert!(
        values.iter().all(|v| !v.starts_with(b"zombie-")),
        "zombie's acks=1 divergence must have been truncated"
    );
}
