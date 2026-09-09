//! Kafka-API target and workload (Redpanda, Apache Kafka) via librdkafka.
//!
//! Leadership is observed through the Kafka Metadata API, the same way any
//! client learns it. `leaders()` uses one long-lived client bootstrapped from
//! every broker: whichever live broker librdkafka picks answers, which is
//! exactly "what a client would learn on refresh". `leaders_via(i)` creates a
//! throw-away client that knows only broker `i`, so its first metadata answer
//! is that broker's own opinion — on an isolated zombie, a stale one.

use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, Offset, TopicPartitionList};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::cluster::{Fault, FaultTarget, majority_leaders};
use super::docker::Docker;
use super::kmeta;
use super::workload::{AckLevel, Reader, Workload, decode_id, encode_value};

#[derive(Clone, Debug)]
pub struct KafkaTargetSpec {
    pub docker: Docker,
    /// Host-reachable client address per broker (`localhost:29090`, ...).
    pub client_addrs: Vec<String>,
    /// Redpanda admin API per broker (`localhost:29640`, ...); empty for
    /// Apache Kafka, which then uses the ISR view from metadata for health.
    pub admin_addrs: Vec<String>,
    /// Kafka node id of each broker index.
    pub node_ids: Vec<i32>,
}

pub struct KafkaTarget {
    spec: KafkaTargetSpec,
}

fn base_config(bootstrap: &str) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", bootstrap);
    c.set("socket.timeout.ms", "1000");
    c.set("socket.connection.setup.timeout.ms", "1000");
    c.set("topic.metadata.refresh.interval.ms", "1000");
    c.set("log_level", "3");
    c
}

impl KafkaTarget {
    pub fn new(spec: KafkaTargetSpec) -> Result<KafkaTarget, String> {
        Ok(KafkaTarget { spec })
    }

    fn index_of(&self, node_id: i32) -> i32 {
        self.spec
            .node_ids
            .iter()
            .position(|&n| n == node_id)
            .map(|i| i as i32)
            .unwrap_or(-1)
    }

    fn leaders_from(&self, view: &kmeta::MetadataView, partitions: u32) -> Vec<i32> {
        let mut out = vec![-1; partitions as usize];
        for p in &view.partitions {
            if p.error_code == 0 && (p.partition as usize) < out.len() && p.partition >= 0 {
                out[p.partition as usize] = self.index_of(p.leader);
            }
        }
        out
    }

    /// Redpanda admin API: `GET /v1/brokers` on any live node.
    async fn brokers_json(&self) -> Option<String> {
        for addr in &self.spec.admin_addrs {
            if let Ok(body) = http_get(addr, "/v1/brokers").await {
                return Some(body);
            }
        }
        None
    }

    /// Redpanda admin API: `GET /v1/cluster/health_overview` on any live node.
    async fn health_overview(&self) -> Option<String> {
        for addr in &self.spec.admin_addrs {
            if let Ok(body) = http_get(addr, "/v1/cluster/health_overview").await {
                return Some(body);
            }
        }
        None
    }
}

pub(crate) async fn http_get(addr: &str, path: &str) -> Result<String, String> {
    let io = async {
        let mut s = tokio::net::TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
        s.write_all(format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&buf).to_string();
        let (head, body) = text.split_once("\r\n\r\n").ok_or("no http body")?;
        if !head.starts_with("HTTP/1.0 200") && !head.starts_with("HTTP/1.1 200") {
            return Err(format!("http {}", head.lines().next().unwrap_or("")));
        }
        Ok(body.to_string())
    };
    tokio::time::timeout(Duration::from_secs(2), io)
        .await
        .map_err(|_| "http timeout".to_string())?
}

/// Tiny JSON field readers for the admin API's flat health object — enough
/// for `"leaderless_count": 0` and `"nodes_down": []`.
pub(crate) fn json_number(body: &str, key: &str) -> Option<i64> {
    let idx = body.find(&format!("\"{key}\""))?;
    let rest = body[idx + key.len() + 2..].trim_start().strip_prefix(':')?.trim_start();
    let end = rest.find(|c: char| !(c.is_ascii_digit() || c == '-')).unwrap_or(rest.len());
    rest[..end].parse().ok()
}

pub(crate) fn json_array_len(body: &str, key: &str) -> Option<usize> {
    let idx = body.find(&format!("\"{key}\""))?;
    let rest = body[idx + key.len() + 2..].trim_start().strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('[')?;
    let inner = rest[..rest.find(']')?].trim();
    Some(if inner.is_empty() { 0 } else { inner.split(',').count() })
}

impl FaultTarget for KafkaTarget {
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
        let admin: AdminClient<DefaultClientContext> = base_config(&self.spec.client_addrs.join(","))
            .create()
            .map_err(|e| e.to_string())?;
        let mut last_err = String::new();
        for attempt in 0..5 {
            let new_topic = NewTopic::new(topic, partitions as i32, TopicReplication::Fixed(replication as i32))
                .set("min.insync.replicas", "2");
            let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
            match admin.create_topics(&[new_topic], &opts).await {
                Ok(results) => {
                    let mut ok = true;
                    for r in results {
                        match r {
                            Ok(_) => {}
                            Err((name, code)) if code == rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists => {
                                eprintln!("topic {name} already exists; reusing");
                            }
                            Err((name, code)) => {
                                ok = false;
                                last_err = format!("create topic {name}: {code:?}");
                            }
                        }
                    }
                    if ok {
                        break;
                    }
                }
                Err(e) => last_err = e.to_string(),
            }
            eprintln!("create_topic attempt {attempt} failed: {last_err}; retrying");
            tokio::time::sleep(Duration::from_secs(2)).await;
            if attempt == 4 {
                return Err(last_err);
            }
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
        let view = kmeta::fetch(&self.spec.client_addrs[i], topic, Duration::from_millis(500)).await?;
        Ok(self.leaders_from(&view, partitions))
    }

    async fn wait_healthy(&self, topic: &str, partitions: u32, timeout: Duration) -> Result<(), String> {
        // Health must hold on two consecutive reads 500 ms apart: right after
        // a kill the admin API can still report the pre-fault picture, and a
        // single green read would let the next fault land on a cluster that
        // has not actually re-formed.
        let deadline = Instant::now() + timeout;
        let n = self.broker_count();
        let mut streak = 0;
        loop {
            let healthy = if self.spec.admin_addrs.is_empty() {
                // Apache Kafka: every broker answers, and every one of them
                // sees a leader and a full ISR on every partition.
                let mut all = true;
                for i in 0..n {
                    match kmeta::fetch(&self.spec.client_addrs[i], topic, Duration::from_millis(500)).await {
                        Ok(v) => {
                            all &= v.brokers.len() == n
                                && v.partitions.len() == partitions as usize
                                && v.partitions.iter().all(|p| {
                                    p.error_code == 0 && p.leader >= 0 && p.isr.len() == p.replicas.len()
                                });
                        }
                        Err(_) => all = false,
                    }
                }
                all
            } else {
                let overview = self.health_overview().await;
                let brokers = self.brokers_json().await;
                match (overview, brokers) {
                    (Some(body), Some(brokers)) => {
                        body.contains("\"is_healthy\": true")
                            && json_number(&body, "leaderless_count") == Some(0)
                            && json_number(&body, "under_replicated_count") == Some(0)
                            && json_array_len(&body, "nodes_down") == Some(0)
                            && json_array_len(&body, "all_nodes") == Some(n)
                            && brokers.matches("\"is_alive\": true").count() == n
                            && brokers.matches("\"membership_status\": \"active\"").count() == n
                    }
                    _ => false,
                }
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

pub struct KafkaWorkload {
    bootstrap: String,
    all: FutureProducer,
    leader_only: FutureProducer,
    value_bytes: usize,
}

impl KafkaWorkload {
    pub fn new(client_addrs: &[String], value_bytes: usize) -> Result<KafkaWorkload, String> {
        let bootstrap = client_addrs.join(",");
        let make = |acks: &str| -> Result<FutureProducer, String> {
            let mut c = base_config(&bootstrap);
            c.set("acks", acks);
            c.set("enable.idempotence", "false");
            c.set("message.timeout.ms", "15000");
            c.set("request.timeout.ms", "5000");
            c.set("retry.backoff.ms", "100");
            c.set("linger.ms", "5");
            c.create().map_err(|e| e.to_string())
        };
        Ok(KafkaWorkload {
            all: make("all")?,
            leader_only: make("1")?,
            bootstrap,
            value_bytes,
        })
    }
}

impl Workload for KafkaWorkload {
    type Reader = KafkaReader;

    async fn produce(&self, topic: &str, partition: u32, ids: &[u64], acks: AckLevel) -> Vec<u64> {
        let producer = match acks {
            AckLevel::All => &self.all,
            AckLevel::LeaderOnly => &self.leader_only,
        };
        let values: Vec<Vec<u8>> = ids.iter().map(|&id| encode_value(id, self.value_bytes)).collect();
        let sends = values.iter().map(|v| {
            producer.send(
                FutureRecord::<(), [u8]>::to(topic).partition(partition as i32).payload(&v[..]),
                Duration::from_secs(15),
            )
        });
        let results = join_all(sends).await;
        ids.iter()
            .zip(results)
            .filter_map(|(&id, r)| r.ok().map(|_| id))
            .collect()
    }

    async fn reader(&self, topic: &str, partition: u32, name: &str) -> Result<KafkaReader, String> {
        let mut c = base_config(&self.bootstrap);
        c.set("group.id", format!("torture-{name}"));
        c.set("enable.auto.commit", "false");
        c.set("auto.offset.reset", "earliest");
        c.set("enable.partition.eof", "false");
        c.set("fetch.wait.max.ms", "50");
        let consumer: StreamConsumer = c.create().map_err(|e| e.to_string())?;
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition as i32, Offset::Beginning)
            .map_err(|e| e.to_string())?;
        consumer.assign(&tpl).map_err(|e| e.to_string())?;
        Ok(KafkaReader { consumer })
    }
}

/// Await a batch of futures without pulling in the `futures` crate.
async fn join_all<F: std::future::Future>(iter: impl Iterator<Item = F>) -> Vec<F::Output> {
    let mut futs: Vec<std::pin::Pin<Box<F>>> = iter.map(Box::pin).collect();
    let mut out: Vec<Option<F::Output>> = (0..futs.len()).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut all_done = true;
        for (i, f) in futs.iter_mut().enumerate() {
            if out[i].is_none() {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(v) => out[i] = Some(v),
                    std::task::Poll::Pending => all_done = false,
                }
            }
        }
        if all_done { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
    })
    .await;
    out.into_iter().map(|o| o.unwrap()).collect()
}

pub struct KafkaReader {
    consumer: StreamConsumer,
}

impl Reader for KafkaReader {
    async fn next(&mut self) -> Result<Vec<(u64, u64)>, String> {
        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        loop {
            match tokio::time::timeout_at(deadline, self.consumer.recv()).await {
                Ok(Ok(msg)) => {
                    if let Some(id) = msg.payload().and_then(decode_id) {
                        out.push((msg.offset() as u64, id));
                    }
                    if out.len() >= 1000 {
                        return Ok(out);
                    }
                }
                Ok(Err(e)) => {
                    return if out.is_empty() { Err(e.to_string()) } else { Ok(out) };
                }
                Err(_) => return Ok(out),
            }
        }
    }
}
