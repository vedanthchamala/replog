//! replog as a fault-injection target (containers) and workload.
//!
//! Same Docker backend and network layout as the Kafka-API targets: brokers
//! advertise a host-published client address and a private peers address,
//! so `isolate` cuts a broker off from the controller *and* from replica
//! fetch while the workload on the host keeps reaching it. Leadership is read
//! the way clients read it (a `Metadata` request to one named broker);
//! health comes from the controller's view (full ISR on every partition).

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::{ClusterClient, Connection};
use crate::harness::isr_of;
use crate::proto::{Acks, ErrorCode, ProduceRecord, Request, Response};

use super::cluster::{Fault, FaultTarget, majority_leaders};
use super::docker::Docker;
use super::workload::{AckLevel, Reader, Workload, decode_id, encode_value};

#[derive(Clone, Debug)]
pub struct ReplogTargetSpec {
    pub docker: Docker,
    /// Host-reachable client address per broker (`localhost:29000`, ...).
    pub client_addrs: Vec<String>,
    /// Host-reachable controller address (`localhost:29099`).
    pub controller_addr: String,
}

pub struct ReplogTarget {
    spec: ReplogTargetSpec,
}

impl ReplogTarget {
    pub fn new(spec: ReplogTargetSpec) -> ReplogTarget {
        ReplogTarget { spec }
    }

    async fn controller_meta(&self) -> Result<crate::proto::ClusterMeta, String> {
        let io = async {
            let conn = Connection::connect(&self.spec.controller_addr)
                .await
                .map_err(|e| e.to_string())?;
            match conn.call(&Request::ControllerMetadata).await {
                Ok(Response::ControllerMetadata {
                    error: ErrorCode::None,
                    cluster,
                }) => Ok(cluster),
                other => Err(format!("controller metadata: {other:?}")),
            }
        };
        tokio::time::timeout(Duration::from_secs(2), io)
            .await
            .map_err(|_| "controller metadata timeout".to_string())?
    }
}

impl FaultTarget for ReplogTarget {
    fn broker_count(&self) -> usize {
        self.spec.client_addrs.len()
    }

    fn broker_name(&self, i: usize) -> String {
        self.spec.docker.containers[i].clone()
    }

    fn client_addr(&self, i: usize) -> String {
        self.spec.client_addrs[i].clone()
    }

    async fn fault(&self, i: usize, f: Fault) -> Result<(), String> {
        self.spec.docker.apply(i, f).await
    }

    async fn heal(&self, i: usize, f: Fault) -> Result<(), String> {
        self.spec.docker.heal(i, f).await
    }

    async fn create_topic(&self, topic: &str, partitions: u32, replication: u32) -> Result<(), String> {
        let addrs: Vec<&str> = self.spec.client_addrs.iter().map(|s| s.as_str()).collect();
        let client = ClusterClient::connect(&addrs).await.map_err(|e| e.to_string())?;
        match client.create_topic(topic, partitions, replication).await {
            Ok(()) => {}
            Err(crate::client::ClientError::Broker(ErrorCode::TopicExists)) => {
                eprintln!("topic {topic} already exists; reusing");
            }
            Err(e) => return Err(format!("create topic: {e}")),
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let l = majority_leaders(self, topic, partitions).await;
            if l.iter().all(|&x| x >= 0) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("topic {topic} partitions leaderless after create: {l:?}"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn view_via(&self, i: usize, topic: &str, partitions: u32) -> Result<Vec<i32>, String> {
        let addr = self.spec.client_addrs[i].clone();
        let io = async {
            let conn = Connection::connect(&addr).await.map_err(|e| e.to_string())?;
            conn.metadata().await.map_err(|e| e.to_string())
        };
        let meta = tokio::time::timeout(Duration::from_millis(500), io)
            .await
            .map_err(|_| format!("metadata {addr}: timeout"))??;
        let mut out = vec![-1; partitions as usize];
        if let Some(t) = meta.topics.iter().find(|t| t.name == topic) {
            for p in &t.partitions {
                if (p.partition as usize) < out.len() {
                    out[p.partition as usize] = p.leader;
                }
            }
        }
        Ok(out)
    }

    async fn wait_healthy(&self, topic: &str, partitions: u32, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        let n = self.broker_count();
        let mut streak = 0;
        loop {
            let healthy = match self.controller_meta().await {
                Ok(m) => {
                    m.brokers.len() == n
                        && (0..partitions).all(|p| isr_of(&m, topic, p).len() == n)
                }
                Err(_) => false,
            };
            streak = if healthy { streak + 1 } else { 0 };
            if streak >= 2 {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("cluster not healthy within {timeout:?}"));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

// ---------------------------------------------------------------- workload

pub struct ReplogWorkload {
    client: Arc<ClusterClient>,
    value_bytes: usize,
}

impl ReplogWorkload {
    pub async fn new(client_addrs: &[String], value_bytes: usize) -> Result<ReplogWorkload, String> {
        let addrs: Vec<&str> = client_addrs.iter().map(|s| s.as_str()).collect();
        let client = ClusterClient::connect(&addrs).await.map_err(|e| e.to_string())?;
        Ok(ReplogWorkload {
            client: Arc::new(client),
            value_bytes,
        })
    }
}

impl Workload for ReplogWorkload {
    type Reader = ReplogReader;

    async fn produce(&self, topic: &str, partition: u32, ids: &[u64], acks: AckLevel) -> Vec<u64> {
        let acks = match acks {
            AckLevel::All => Acks::All,
            AckLevel::LeaderOnly => Acks::Written,
        };
        let records: Vec<ProduceRecord> = ids
            .iter()
            .map(|&id| ProduceRecord {
                key: None,
                value: encode_value(id, self.value_bytes),
            })
            .collect();
        match self.client.produce(topic, partition, acks, records).await {
            Ok(_) => ids.to_vec(),
            Err(_) => Vec::new(),
        }
    }

    async fn reader(&self, topic: &str, partition: u32, _name: &str) -> Result<ReplogReader, String> {
        Ok(ReplogReader {
            client: self.client.clone(),
            topic: topic.to_string(),
            partition,
            position: 0,
        })
    }
}

pub struct ReplogReader {
    client: Arc<ClusterClient>,
    topic: String,
    partition: u32,
    position: u64,
}

impl Reader for ReplogReader {
    async fn next(&mut self) -> Result<Vec<(u64, u64)>, String> {
        match self
            .client
            .fetch(&self.topic, self.partition, self.position, 256 << 10, 50)
            .await
        {
            Ok((_, _, records)) => {
                let mut out = Vec::with_capacity(records.len());
                for r in &records {
                    if let Some(id) = decode_id(&r.value) {
                        out.push((r.offset, id));
                    }
                    self.position = r.offset + 1;
                }
                Ok(out)
            }
            Err(e) => Err(e.to_string()),
        }
    }
}
