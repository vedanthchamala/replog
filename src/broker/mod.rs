//! The broker: a tokio TCP server in front of per-partition writer threads.
//!
//! Request flow: connection task decodes a frame and *synchronously* enqueues
//! partition commands (so pipelined produces from one client keep their order),
//! then awaits replies in spawned tasks so a slow ack or a parked long-poll
//! fetch never blocks the connection's other requests. Responses are funneled
//! through one writer task per connection and may interleave out of request
//! order — correlation IDs are what make that safe.

mod groups;
mod offsets;
mod partition;

pub use partition::PartitionHandle;

use groups::GroupCoordinator;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use crate::proto::{
    self, Acks, ClusterMeta, ErrorCode, FetchedRecord, PartitionMeta, Request, Response,
    TopicMeta,
};
use crate::storage::LogConfig;
use offsets::{OFFSETS_TOPIC, OffsetsStore};

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage: {0}")]
    Storage(#[from] crate::storage::StorageError),
    #[error("invalid data dir layout: {0}")]
    InvalidLayout(String),
    #[error("offsets replay failed: {0:?}")]
    OffsetsReplay(ErrorCode),
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub log: LogConfig,
}

struct Shared {
    config: BrokerConfig,
    advertised: String,
    topics: RwLock<HashMap<String, Arc<Vec<PartitionHandle>>>>,
    offsets: OffsetsStore,
    groups: GroupCoordinator,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

pub struct BrokerHandle {
    pub addr: SocketAddr,
    shared: Arc<Shared>,
    shutdown_tx: watch::Sender<bool>,
    accept_task: JoinHandle<()>,
    expiry_task: JoinHandle<()>,
}

pub struct Broker;

impl Broker {
    pub async fn start(listen: &str, config: BrokerConfig) -> Result<BrokerHandle, BrokerError> {
        std::fs::create_dir_all(&config.data_dir)?;
        let mut threads = Vec::new();
        let mut topics: HashMap<String, Vec<PartitionHandle>> = HashMap::new();

        for (topic, partitions) in scan_data_dir(&config.data_dir)? {
            let mut handles = Vec::with_capacity(partitions as usize);
            for p in 0..partitions {
                let dir = config.data_dir.join(format!("{topic}-{p}"));
                let (handle, join) = partition::spawn(&dir, config.log.clone())?;
                handles.push(handle);
                threads.push(join);
            }
            topics.insert(topic, handles);
        }

        let offsets_handle = match topics.remove(OFFSETS_TOPIC) {
            Some(mut handles) => handles.remove(0),
            None => {
                let dir = config.data_dir.join(format!("{OFFSETS_TOPIC}-0"));
                let (handle, join) = partition::spawn(&dir, config.log.clone())?;
                threads.push(join);
                handle
            }
        };
        let offsets = OffsetsStore::open(offsets_handle)
            .await
            .map_err(BrokerError::OffsetsReplay)?;

        let listener = TcpListener::bind(listen).await?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared {
            config,
            advertised: addr.to_string(),
            topics: RwLock::new(
                topics
                    .into_iter()
                    .map(|(k, v)| (k, Arc::new(v)))
                    .collect(),
            ),
            offsets,
            groups: GroupCoordinator::new(),
            threads: Mutex::new(threads),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let expiry_shared = shared.clone();
        let mut expiry_shutdown = shutdown_rx.clone();
        let expiry_task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(200));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let counts = |t: &str| expiry_shared.topic_partitions(t);
                        expiry_shared.groups.expire(&counts);
                    }
                    _ = expiry_shutdown.changed() => break,
                }
            }
        });

        let accept_shared = shared.clone();
        let accept_task = tokio::spawn(async move {
            let mut conns = JoinSet::new();
            let mut shutdown = shutdown_rx.clone();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, _)) => {
                                conns.spawn(serve_connection(
                                    accept_shared.clone(),
                                    stream,
                                    shutdown_rx.clone(),
                                ));
                            }
                            Err(e) => {
                                eprintln!("accept failed: {e}");
                                break;
                            }
                        }
                    }
                    _ = shutdown.changed() => break,
                }
            }
            while conns.join_next().await.is_some() {}
        });

        Ok(BrokerHandle {
            addr,
            shared,
            shutdown_tx,
            accept_task,
            expiry_task,
        })
    }
}

impl BrokerHandle {
    /// Graceful stop: closes connections, then joins every partition thread
    /// (each does a final flush on the way out).
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.accept_task.await;
        let _ = self.expiry_task.await;
        let mut shared = self.shared;
        let inner = loop {
            match Arc::try_unwrap(shared) {
                Ok(inner) => break inner,
                Err(still_shared) => {
                    shared = still_shared;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        };
        let Shared {
            topics,
            offsets,
            threads,
            ..
        } = inner;
        drop(topics);
        drop(offsets);
        let threads = threads.into_inner().unwrap();
        let _ = tokio::task::spawn_blocking(move || {
            for t in threads {
                let _ = t.join();
            }
        })
        .await;
    }
}

fn scan_data_dir(dir: &PathBuf) -> Result<Vec<(String, u32)>, BrokerError> {
    let mut partitions: HashMap<String, Vec<u64>> = HashMap::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        if !entry.file_type()?.is_dir() {
            return Err(BrokerError::InvalidLayout(format!(
                "unexpected file {name} in data dir"
            )));
        }
        let Some((topic, digits)) = name.rsplit_once('-') else {
            return Err(BrokerError::InvalidLayout(format!(
                "directory {name} is not <topic>-<partition>"
            )));
        };
        let Ok(p) = digits.parse::<u64>() else {
            return Err(BrokerError::InvalidLayout(format!(
                "directory {name} has a non-numeric partition suffix"
            )));
        };
        partitions.entry(topic.to_string()).or_default().push(p);
    }
    let mut out = Vec::new();
    for (topic, mut ps) in partitions {
        ps.sort_unstable();
        for (want, &have) in ps.iter().enumerate() {
            if want as u64 != have {
                return Err(BrokerError::InvalidLayout(format!(
                    "topic {topic} partitions are not contiguous from 0: {ps:?}"
                )));
            }
        }
        out.push((topic, ps.len() as u32));
    }
    Ok(out)
}

fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 200
        && !name.starts_with("__")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

impl Shared {
    fn topic_partitions(&self, topic: &str) -> Option<u32> {
        self.topics
            .read()
            .unwrap()
            .get(topic)
            .map(|ps| ps.len() as u32)
    }

    fn partition(&self, topic: &str, partition: u32) -> Option<PartitionHandle> {
        self.topics
            .read()
            .unwrap()
            .get(topic)
            .and_then(|ps| ps.get(partition as usize))
            .cloned()
    }

    fn create_topic(&self, topic: &str, partitions: u32) -> ErrorCode {
        if !valid_topic_name(topic) || partitions == 0 || partitions > 1024 {
            return ErrorCode::Malformed;
        }
        let mut topics = self.topics.write().unwrap();
        if topics.contains_key(topic) {
            return ErrorCode::TopicExists;
        }
        let mut handles = Vec::with_capacity(partitions as usize);
        let mut joins = Vec::with_capacity(partitions as usize);
        for p in 0..partitions {
            let dir = self.config.data_dir.join(format!("{topic}-{p}"));
            match partition::spawn(&dir, self.config.log.clone()) {
                Ok((handle, join)) => {
                    handles.push(handle);
                    joins.push(join);
                }
                Err(e) => {
                    eprintln!("create_topic {topic}: {e}");
                    return ErrorCode::Storage;
                }
            }
        }
        topics.insert(topic.to_string(), Arc::new(handles));
        self.threads.lock().unwrap().extend(joins);
        ErrorCode::None
    }

    /// Standalone-mode metadata: this broker (id 0) leads every partition at
    /// epoch 0 with itself as the only replica.
    fn metadata(&self) -> ClusterMeta {
        let mut topics: Vec<TopicMeta> = self
            .topics
            .read()
            .unwrap()
            .iter()
            .map(|(name, ps)| TopicMeta {
                name: name.clone(),
                partitions: (0..ps.len() as u32)
                    .map(|partition| PartitionMeta {
                        partition,
                        leader: 0,
                        leader_epoch: 0,
                        replicas: vec![0],
                        isr: vec![0],
                    })
                    .collect(),
            })
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        ClusterMeta {
            version: 0,
            brokers: vec![(0, self.advertised.clone())],
            topics,
        }
    }
}

async fn serve_connection(
    shared: Arc<Shared>,
    stream: TcpStream,
    mut shutdown: watch::Receiver<bool>,
) {
    if let Err(e) = stream.set_nodelay(true) {
        eprintln!("set_nodelay failed: {e}");
    }
    let (mut rd, mut wr) = stream.into_split();
    let (out_tx, mut out_rx) = unbounded_channel::<Vec<u8>>();
    let writer = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            if proto::write_frame(&mut wr, &frame).await.is_err() {
                break;
            }
        }
    });

    loop {
        let frame = tokio::select! {
            read = proto::read_frame(&mut rd) => match read {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("connection read failed: {e}");
                    break;
                }
            },
            _ = shutdown.changed() => break,
        };
        match proto::decode_request(&frame) {
            Ok((corr, req)) => dispatch(&shared, req, corr, &out_tx),
            Err(e) => {
                eprintln!("undecodable request frame ({e}); closing connection");
                break;
            }
        }
    }

    drop(out_tx);
    let _ = writer.await;
}

fn respond(out: &UnboundedSender<Vec<u8>>, resp: &Response, corr: u32) {
    let _ = out.send(proto::encode_response(resp, corr));
}

/// Decodes-and-routes one request. Partition commands are enqueued here, in
/// the connection task, so a client's pipelined produces stay ordered; only
/// the reply-awaiting happens in spawned tasks.
fn dispatch(shared: &Arc<Shared>, req: Request, corr: u32, out: &UnboundedSender<Vec<u8>>) {
    match req {
        Request::CreateTopic {
            topic,
            partitions,
            replication_factor,
        } => {
            // Standalone broker cannot host replicas; a cluster routes topic
            // creation through the controller instead.
            let error = if replication_factor > 1 {
                ErrorCode::Malformed
            } else {
                shared.create_topic(&topic, partitions)
            };
            respond(out, &Response::CreateTopic { error }, corr);
        }
        Request::Metadata => {
            respond(
                out,
                &Response::Metadata {
                    error: ErrorCode::None,
                    cluster: shared.metadata(),
                },
                corr,
            );
        }
        Request::Produce {
            topic,
            partition,
            acks,
            leader_epoch,
            records,
        } => {
            if leader_epoch != 0 {
                if acks != Acks::None {
                    respond(
                        out,
                        &Response::Produce {
                            error: ErrorCode::FencedEpoch,
                            base_offset: 0,
                            count: 0,
                        },
                        corr,
                    );
                }
                return;
            }
            let Some(handle) = shared.partition(&topic, partition) else {
                if acks != Acks::None {
                    respond(
                        out,
                        &Response::Produce {
                            error: ErrorCode::UnknownTopicOrPartition,
                            base_offset: 0,
                            count: 0,
                        },
                        corr,
                    );
                }
                return;
            };
            let records: Vec<_> = records.into_iter().map(|r| (r.key, r.value)).collect();
            if acks == Acks::None {
                handle.append_no_reply(records);
                return;
            }
            let reply = handle.append_start(records, acks);
            let out = out.clone();
            tokio::spawn(async move {
                let resp = match reply.await.unwrap_or(Err(ErrorCode::Storage)) {
                    Ok((base_offset, count)) => Response::Produce {
                        error: ErrorCode::None,
                        base_offset,
                        count,
                    },
                    Err(error) => Response::Produce {
                        error,
                        base_offset: 0,
                        count: 0,
                    },
                };
                respond(&out, &resp, corr);
            });
        }
        Request::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
            max_wait_ms,
        } => {
            let Some(handle) = shared.partition(&topic, partition) else {
                respond(
                    out,
                    &Response::Fetch {
                        error: ErrorCode::UnknownTopicOrPartition,
                        log_start: 0,
                        next_offset: 0,
                        records: Vec::new(),
                    },
                    corr,
                );
                return;
            };
            let out = out.clone();
            tokio::spawn(async move {
                let resp = handle_fetch(handle, offset, max_bytes, max_wait_ms).await;
                respond(&out, &resp, corr);
            });
        }
        Request::CommitOffset {
            group,
            topic,
            partition,
            offset,
            member_id,
            generation,
        } => {
            let fence = shared.groups.check_commit(&group, &member_id, generation);
            if fence != ErrorCode::None {
                respond(out, &Response::CommitOffset { error: fence }, corr);
                return;
            }
            let shared = shared.clone();
            let out = out.clone();
            tokio::spawn(async move {
                let error = shared.offsets.commit(group, topic, partition, offset).await;
                respond(&out, &Response::CommitOffset { error }, corr);
            });
        }
        Request::FetchOffset {
            group,
            topic,
            partition,
        } => {
            let offset = shared
                .offsets
                .fetch(&group, &topic, partition)
                .map_or(-1, |o| o as i64);
            respond(
                out,
                &Response::FetchOffset {
                    error: ErrorCode::None,
                    offset,
                },
                corr,
            );
        }
        Request::JoinGroup {
            group,
            member_id,
            session_timeout_ms,
            topics,
        } => {
            let counts = |t: &str| shared.topic_partitions(t);
            let outcome = shared
                .groups
                .join(&group, &member_id, session_timeout_ms, topics, &counts);
            respond(
                out,
                &Response::JoinGroup {
                    error: ErrorCode::None,
                    member_id: outcome.member_id,
                    generation: outcome.generation,
                    assignment: outcome.assignment,
                },
                corr,
            );
        }
        Request::Heartbeat {
            group,
            member_id,
            generation,
        } => {
            let error = shared.groups.heartbeat(&group, &member_id, generation);
            respond(out, &Response::Heartbeat { error }, corr);
        }
        Request::LeaveGroup { group, member_id } => {
            let counts = |t: &str| shared.topic_partitions(t);
            let error = shared.groups.leave(&group, &member_id, &counts);
            respond(out, &Response::LeaveGroup { error }, corr);
        }
        // Replication traffic reaches a standalone broker only by mistake.
        Request::ReplicaFetch { .. } => {
            respond(
                out,
                &Response::ReplicaFetch {
                    error: ErrorCode::NotLeader,
                    leader_epoch: 0,
                    log_end: 0,
                    high_watermark: 0,
                    records: Vec::new(),
                },
                corr,
            );
        }
        Request::EpochCheck { .. } => {
            respond(
                out,
                &Response::EpochCheck {
                    error: ErrorCode::NotLeader,
                    end_offset: 0,
                },
                corr,
            );
        }
        // Controller-plane messages belong to the controller process.
        Request::RegisterBroker { .. } => {
            respond(
                out,
                &Response::RegisterBroker {
                    error: ErrorCode::Malformed,
                    metadata_version: 0,
                },
                corr,
            );
        }
        Request::BrokerHeartbeat { .. } => {
            respond(
                out,
                &Response::BrokerHeartbeat {
                    error: ErrorCode::Malformed,
                    metadata_version: 0,
                },
                corr,
            );
        }
        Request::AlterIsr { .. } => {
            respond(
                out,
                &Response::AlterIsr {
                    error: ErrorCode::Malformed,
                    metadata_version: 0,
                },
                corr,
            );
        }
        Request::ControllerMetadata => {
            respond(
                out,
                &Response::ControllerMetadata {
                    error: ErrorCode::Malformed,
                    cluster: ClusterMeta::default(),
                },
                corr,
            );
        }
    }
}

async fn handle_fetch(
    handle: PartitionHandle,
    offset: u64,
    max_bytes: u32,
    max_wait_ms: u32,
) -> Response {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(max_wait_ms as u64);
    let mut next_offset = handle.next_offset.clone();
    let result = loop {
        match handle.read(offset, max_bytes as u64).await {
            Ok(ok) if ok.records.is_empty() && tokio::time::Instant::now() < deadline => {
                let woke = tokio::time::timeout_at(deadline, next_offset.wait_for(|&n| n > offset))
                    .await;
                match woke {
                    Ok(Ok(_)) => continue,
                    _ => break Ok(ok),
                }
            }
            other => break other,
        }
    };
    match result {
        Ok(ok) => Response::Fetch {
            error: ErrorCode::None,
            log_start: ok.log_start,
            next_offset: ok.next_offset,
            records: ok
                .records
                .into_iter()
                .map(|r| FetchedRecord {
                    offset: r.offset,
                    timestamp_ms: r.timestamp_ms,
                    key: r.key,
                    value: r.value,
                })
                .collect(),
        },
        Err(error) => Response::Fetch {
            error,
            log_start: 0,
            next_offset: 0,
            records: Vec::new(),
        },
    }
}
