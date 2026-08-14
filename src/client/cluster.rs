//! Cluster-aware client: routes to partition leaders and retries through
//! failovers. The retry is what makes a leader election invisible to the
//! workload — and what makes duplicates possible (a request may have been
//! applied before its connection died), which is why the delivery contract is
//! at-least-once and the checker counts duplicates instead of denying them.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::client::{ClientError, Connection, Result};
use crate::proto::{Acks, ClusterMeta, ErrorCode, FetchedRecord, ProduceRecord};

pub struct ClusterClient {
    bootstrap: Vec<String>,
    conns: Mutex<HashMap<String, Connection>>,
    meta: std::sync::Mutex<ClusterMeta>,
    pub retry_timeout: Duration,
    pub retry_backoff: Duration,
}

impl ClusterClient {
    pub async fn connect(bootstrap: &[&str]) -> Result<Self> {
        let client = Self {
            bootstrap: bootstrap.iter().map(|s| s.to_string()).collect(),
            conns: Mutex::new(HashMap::new()),
            meta: std::sync::Mutex::new(ClusterMeta::default()),
            retry_timeout: Duration::from_secs(15),
            retry_backoff: Duration::from_millis(100),
        };
        client.refresh_metadata().await?;
        Ok(client)
    }

    async fn conn_to(&self, addr: &str) -> Result<Connection> {
        let mut conns = self.conns.lock().await;
        if let Some(c) = conns.get(addr) {
            return Ok(c.clone());
        }
        let c = Connection::connect(addr).await?;
        conns.insert(addr.to_string(), c.clone());
        Ok(c)
    }

    async fn drop_conn(&self, addr: &str) {
        self.conns.lock().await.remove(addr);
    }

    /// Asks every reachable known address and keeps the *newest* metadata by
    /// version. First-answer-wins is wrong here: a deposed leader cut off
    /// from the controller still answers metadata requests with its stale
    /// view, and if it happens to sit first in the candidate list it would
    /// keep routing this client to itself (found by the Stage 5 torture
    /// harness's zombie-leader scenario). Versions are monotonic at the
    /// controller, so newest-wins converges on the live topology.
    pub async fn refresh_metadata(&self) -> Result<()> {
        let mut candidates: Vec<String> = self.bootstrap.clone();
        candidates.extend(
            self.meta
                .lock()
                .unwrap()
                .brokers
                .iter()
                .map(|(_, addr)| addr.clone()),
        );
        let mut seen = std::collections::HashSet::new();
        candidates.retain(|addr| seen.insert(addr.clone()));
        let mut best: Option<ClusterMeta> = None;
        let mut last_err = ClientError::Closed;
        for addr in candidates {
            match self.conn_to(&addr).await {
                Ok(conn) => match conn.metadata().await {
                    Ok(cluster) => {
                        if best.as_ref().is_none_or(|b| cluster.version > b.version) {
                            best = Some(cluster);
                        }
                    }
                    Err(e) => {
                        self.drop_conn(&addr).await;
                        last_err = e;
                    }
                },
                Err(e) => {
                    self.drop_conn(&addr).await;
                    last_err = e;
                }
            }
        }
        match best {
            Some(cluster) => {
                *self.meta.lock().unwrap() = cluster;
                Ok(())
            }
            None => Err(last_err),
        }
    }

    pub fn metadata(&self) -> ClusterMeta {
        self.meta.lock().unwrap().clone()
    }

    fn leader_for(&self, topic: &str, partition: u32) -> Option<(String, u64)> {
        let meta = self.meta.lock().unwrap();
        let p = meta
            .topics
            .iter()
            .find(|t| t.name == topic)?
            .partitions
            .iter()
            .find(|p| p.partition == partition)?;
        if p.leader < 0 {
            return None;
        }
        let addr = meta
            .brokers
            .iter()
            .find(|(id, _)| *id == p.leader as u32)
            .map(|(_, addr)| addr.clone())?;
        Some((addr, p.leader_epoch))
    }

    pub async fn create_topic(
        &self,
        topic: &str,
        partitions: u32,
        replication_factor: u32,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.retry_timeout;
        loop {
            let addrs: Vec<String> = {
                let meta = self.meta.lock().unwrap();
                meta.brokers.iter().map(|(_, a)| a.clone()).collect()
            };
            let candidates = if addrs.is_empty() {
                self.bootstrap.clone()
            } else {
                addrs
            };
            for addr in candidates {
                if let Ok(conn) = self.conn_to(&addr).await {
                    match conn
                        .create_topic_replicated(topic, partitions, replication_factor)
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(ClientError::Broker(ErrorCode::TopicExists)) => {
                            return Err(ClientError::Broker(ErrorCode::TopicExists));
                        }
                        Err(_) => self.drop_conn(&addr).await,
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ClientError::Closed);
            }
            tokio::time::sleep(self.retry_backoff).await;
            let _ = self.refresh_metadata().await;
        }
    }

    /// Produces with leader routing and retry-through-failover. Returns the
    /// base offset of the last successful attempt.
    pub async fn produce(
        &self,
        topic: &str,
        partition: u32,
        acks: Acks,
        records: Vec<ProduceRecord>,
    ) -> Result<Option<u64>> {
        let deadline = tokio::time::Instant::now() + self.retry_timeout;
        loop {
            let attempt = match self.leader_for(topic, partition) {
                None => Err(ClientError::Broker(ErrorCode::Offline)),
                Some((addr, epoch)) => match self.conn_to(&addr).await {
                    Err(e) => {
                        self.drop_conn(&addr).await;
                        Err(e)
                    }
                    Ok(conn) => {
                        let result = conn
                            .produce_with_epoch(topic, partition, acks, epoch, records.clone())
                            .await;
                        if matches!(result, Err(ClientError::Closed) | Err(ClientError::Io(_))) {
                            self.drop_conn(&addr).await;
                        }
                        result
                    }
                },
            };
            match attempt {
                Ok(base) => return Ok(base),
                Err(
                    ClientError::Broker(
                        ErrorCode::NotLeader
                        | ErrorCode::FencedEpoch
                        | ErrorCode::Offline
                        | ErrorCode::NotEnoughReplicas
                        | ErrorCode::UnknownTopicOrPartition,
                    )
                    | ClientError::Closed
                    | ClientError::Io(_),
                ) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(ClientError::Closed);
                    }
                    tokio::time::sleep(self.retry_backoff).await;
                    let _ = self.refresh_metadata().await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Leader-routed fetch with the same retry discipline.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    ) -> Result<(u64, u64, Vec<FetchedRecord>)> {
        let deadline = tokio::time::Instant::now() + self.retry_timeout;
        loop {
            let attempt = match self.leader_for(topic, partition) {
                None => Err(ClientError::Broker(ErrorCode::Offline)),
                Some((addr, _)) => match self.conn_to(&addr).await {
                    Err(e) => {
                        self.drop_conn(&addr).await;
                        Err(e)
                    }
                    Ok(conn) => {
                        let result = conn
                            .fetch(topic, partition, offset, max_bytes, max_wait_ms)
                            .await;
                        if matches!(result, Err(ClientError::Closed) | Err(ClientError::Io(_))) {
                            self.drop_conn(&addr).await;
                        }
                        result
                    }
                },
            };
            match attempt {
                Ok(ok) => return Ok(ok),
                Err(
                    ClientError::Broker(
                        ErrorCode::NotLeader | ErrorCode::FencedEpoch | ErrorCode::Offline,
                    )
                    | ClientError::Closed
                    | ClientError::Io(_),
                ) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(ClientError::Closed);
                    }
                    tokio::time::sleep(self.retry_backoff).await;
                    let _ = self.refresh_metadata().await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
