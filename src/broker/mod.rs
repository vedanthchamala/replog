//! The broker: a tokio TCP server in front of per-partition writer threads.
//!
//! Request flow: connection task decodes a frame and *synchronously* enqueues
//! partition commands (so pipelined produces from one client keep their order),
//! then awaits replies in spawned tasks so a slow ack or a parked long-poll
//! fetch never blocks the connection's other requests. Responses are funneled
//! through one writer task per connection and may interleave out of request
//! order — correlation IDs are what make that safe.

mod cluster;
mod groups;
mod offsets;
mod partition;

pub use partition::PartitionHandle;

use cluster::Replica;
use groups::GroupCoordinator;

use crate::client::Connection;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

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
    pub broker_id: u32,
    /// None = standalone (Stage 2/3 behavior: this broker is the world).
    pub controller_addr: Option<String>,
    /// Address clients should dial; defaults to the bound one.
    pub advertise_addr: Option<String>,
    /// Address other brokers should dial (follower fetch, epoch checks);
    /// defaults to `advertise_addr`. Separate when peers live on a private
    /// network and clients arrive through a published port.
    pub advertise_peer_addr: Option<String>,
    /// acks=all is refused when the ISR is smaller than this.
    pub min_insync_replicas: u32,
    /// A follower silent for this long is proposed out of the ISR.
    pub replica_lag_ms: u64,
}

impl BrokerConfig {
    pub fn standalone(data_dir: impl Into<PathBuf>, log: LogConfig) -> Self {
        Self {
            data_dir: data_dir.into(),
            log,
            broker_id: 0,
            controller_addr: None,
            advertise_addr: None,
            advertise_peer_addr: None,
            min_insync_replicas: 2,
            replica_lag_ms: 2000,
        }
    }

    pub fn clustered(
        data_dir: impl Into<PathBuf>,
        log: LogConfig,
        broker_id: u32,
        controller_addr: impl Into<String>,
    ) -> Self {
        Self {
            controller_addr: Some(controller_addr.into()),
            broker_id,
            ..Self::standalone(data_dir, log)
        }
    }
}

struct Shared {
    config: BrokerConfig,
    advertised: String,
    topics: RwLock<HashMap<String, Arc<Vec<PartitionHandle>>>>,
    offsets: OffsetsStore,
    groups: GroupCoordinator,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
    // Cluster mode:
    replicas: RwLock<HashMap<(String, u32), Arc<Replica>>>,
    cluster_meta: RwLock<crate::proto::ClusterMeta>,
    controller_conn: RwLock<Option<Connection>>,
    refresh_tx: std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<()>>,
}

impl Shared {
    fn is_clustered(&self) -> bool {
        self.config.controller_addr.is_some()
    }

    fn replica(&self, topic: &str, partition: u32) -> Option<Arc<Replica>> {
        self.replicas
            .read()
            .unwrap()
            .get(&(topic.to_string(), partition))
            .cloned()
    }

    fn ping_refresh(&self) {
        if let Some(tx) = self.refresh_tx.get() {
            let _ = tx.send(());
        }
    }
}

pub struct BrokerHandle {
    pub addr: SocketAddr,
    shared: Arc<Shared>,
    shutdown_tx: watch::Sender<bool>,
    accept_task: JoinHandle<()>,
    expiry_task: JoinHandle<()>,
    cluster_task: Option<JoinHandle<()>>,
}

pub struct Broker;

impl Broker {
    pub async fn start(listen: &str, config: BrokerConfig) -> Result<BrokerHandle, BrokerError> {
        std::fs::create_dir_all(&config.data_dir)?;
        let clustered = config.controller_addr.is_some();
        let mut threads = Vec::new();
        let mut topics: HashMap<String, Vec<PartitionHandle>> = HashMap::new();

        for (topic, partitions) in scan_data_dir(&config.data_dir)? {
            // In cluster mode data partitions are opened lazily as replica
            // roles arrive from the controller; only the local offsets log is
            // opened here.
            if clustered && topic != OFFSETS_TOPIC {
                continue;
            }
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
            replicas: RwLock::new(HashMap::new()),
            cluster_meta: RwLock::new(crate::proto::ClusterMeta::default()),
            controller_conn: RwLock::new(None),
            refresh_tx: std::sync::OnceLock::new(),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let cluster_task = if clustered {
            Some(tokio::spawn(cluster_runtime(
                shared.clone(),
                shutdown_rx.clone(),
            )))
        } else {
            None
        };

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
            cluster_task,
        })
    }
}

impl BrokerHandle {
    /// Crash simulation for in-process tests: tasks are aborted, nothing is
    /// flushed or joined. The broker just stops answering, like a `kill -9`
    /// minus the process boundary.
    pub fn abort(self) {
        self.accept_task.abort();
        self.expiry_task.abort();
        if let Some(task) = self.cluster_task {
            task.abort();
        }
        invalidate_replicas(&self.shared);
        self.shared.replicas.write().unwrap().clear();
        *self.shared.controller_conn.write().unwrap() = None;
        std::mem::forget(self.shared);
    }

    /// Graceful stop: closes connections, then joins every partition thread
    /// (each does a final flush on the way out).
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.accept_task.await;
        let _ = self.expiry_task.await;
        if let Some(task) = self.cluster_task {
            let _ = task.await;
        }
        invalidate_replicas(&self.shared);
        self.shared.replicas.write().unwrap().clear();
        *self.shared.controller_conn.write().unwrap() = None;
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

/// Tells every fetcher to die (they hold partition handles; a partition
/// thread cannot exit while its fetcher lives) and fails parked acks.
fn invalidate_replicas(shared: &Arc<Shared>) {
    for replica in shared.replicas.read().unwrap().values() {
        let failed = {
            let mut st = replica.state.lock().unwrap();
            st.fetcher_generation += 1;
            st.fetcher_alive = false;
            st.is_leader = false;
            std::mem::take(&mut st.pending_all)
        };
        for (_, _, _, reply) in failed {
            let _ = reply.send(Err(ErrorCode::NotLeader));
        }
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
        if self.is_clustered() {
            return self
                .cluster_meta
                .read()
                .unwrap()
                .topics
                .iter()
                .find(|t| t.name == topic)
                .map(|t| t.partitions.len() as u32);
        }
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
            peers: vec![(0, self.advertised.clone())],
            topics,
        }
    }
}

/// Registers with the controller, heartbeats, and applies metadata changes
/// (replica creation, role transitions, fetcher lifecycle, ISR proposals).
async fn cluster_runtime(shared: Arc<Shared>, mut shutdown: watch::Receiver<bool>) {
    let controller_addr = shared
        .config
        .controller_addr
        .clone()
        .expect("cluster runtime requires a controller address");
    let (refresh_tx, mut refresh_rx) = unbounded_channel::<()>();
    let _ = shared.refresh_tx.set(refresh_tx);
    let mut local_version = 0u64;
    let mut registered = false;
    let mut tick = tokio::time::interval(Duration::from_millis(300));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let forced = tokio::select! {
            _ = tick.tick() => false,
            _ = refresh_rx.recv() => true,
            _ = shutdown.changed() => break,
        };

        let conn = {
            let existing = shared.controller_conn.read().unwrap().clone();
            match existing {
                Some(c) => c,
                None => match Connection::connect(&controller_addr).await {
                    Ok(c) => {
                        *shared.controller_conn.write().unwrap() = Some(c.clone());
                        registered = false;
                        c
                    }
                    Err(_) => continue,
                },
            }
        };

        if !registered {
            let advertise = shared
                .config
                .advertise_addr
                .clone()
                .unwrap_or_else(|| shared.advertised.clone());
            let peer_addr = shared
                .config
                .advertise_peer_addr
                .clone()
                .unwrap_or_else(|| advertise.clone());
            match conn
                .call(&Request::RegisterBroker {
                    broker_id: shared.config.broker_id,
                    addr: advertise,
                    peer_addr,
                })
                .await
            {
                Ok(Response::RegisterBroker {
                    error: ErrorCode::None,
                    ..
                }) => registered = true,
                _ => {
                    *shared.controller_conn.write().unwrap() = None;
                    continue;
                }
            }
        }

        let heartbeat = conn
            .call(&Request::BrokerHeartbeat {
                broker_id: shared.config.broker_id,
                metadata_version: local_version,
            })
            .await;
        let remote_version = match heartbeat {
            Ok(Response::BrokerHeartbeat {
                error: ErrorCode::None,
                metadata_version,
            }) => metadata_version,
            Ok(Response::BrokerHeartbeat { .. }) => {
                registered = false;
                continue;
            }
            _ => {
                *shared.controller_conn.write().unwrap() = None;
                continue;
            }
        };

        if remote_version != local_version || forced {
            match conn.call(&Request::ControllerMetadata).await {
                Ok(Response::ControllerMetadata {
                    error: ErrorCode::None,
                    cluster,
                }) => {
                    apply_metadata(&shared, &cluster).await;
                    local_version = cluster.version;
                    *shared.cluster_meta.write().unwrap() = cluster;
                }
                _ => {
                    *shared.controller_conn.write().unwrap() = None;
                    continue;
                }
            }
        }

        isr_maintenance(&shared, &conn).await;
    }
}

async fn apply_metadata(shared: &Arc<Shared>, meta: &crate::proto::ClusterMeta) {
    let self_id = shared.config.broker_id;
    for topic in &meta.topics {
        for p in &topic.partitions {
            if !p.replicas.contains(&self_id) {
                continue;
            }
            let key = (topic.name.clone(), p.partition);
            let replica = match get_or_create_replica(shared, &key) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("replica open failed for {}-{}: {e}", key.0, key.1);
                    continue;
                }
            };
            let becoming_leader = p.leader == self_id as i32;
            let mut failed_acks = Vec::new();
            let (transition, generation) = {
                let mut st = replica.state.lock().unwrap();
                let unchanged = st.epoch == p.leader_epoch && st.is_leader == becoming_leader;
                if unchanged {
                    if st.isr != p.isr {
                        st.isr = p.isr.clone();
                    }
                    // A follower whose fetcher died (leader connection lost)
                    // needs a respawn even without a role change.
                    if !st.is_leader && !st.fetcher_alive && p.leader >= 0 {
                        st.fetcher_generation += 1;
                        st.fetcher_alive = true;
                        (true, st.fetcher_generation)
                    } else {
                        (false, st.fetcher_generation)
                    }
                } else {
                    st.epoch = p.leader_epoch;
                    st.replicas = p.replicas.clone();
                    st.isr = p.isr.clone();
                    st.is_leader = becoming_leader;
                    st.fetcher_generation += 1;
                    if becoming_leader {
                        st.match_offsets.clear();
                        st.leader_since = Instant::now();
                        st.fetcher_alive = false;
                    } else {
                        failed_acks = std::mem::take(&mut st.pending_all);
                        st.fetcher_alive = p.leader >= 0;
                    }
                    (true, st.fetcher_generation)
                }
            };
            for (_, _, _, reply) in failed_acks {
                let _ = reply.send(Err(ErrorCode::NotLeader));
            }
            if !transition {
                replica.advance_hwm(self_id);
                continue;
            }
            if becoming_leader {
                replica.handle.record_epoch(p.leader_epoch);
                replica.advance_hwm(self_id);
            } else if p.leader >= 0 {
                let leader_addr = meta
                    .peers
                    .iter()
                    .chain(meta.brokers.iter())
                    .find(|(id, _)| *id == p.leader as u32)
                    .map(|(_, addr)| addr.clone());
                if let Some(addr) = leader_addr {
                    let refresh = shared.refresh_tx.get().cloned();
                    tokio::spawn(cluster::run_fetcher(
                        replica.clone(),
                        self_id,
                        p.leader_epoch,
                        generation,
                        addr,
                        refresh.expect("refresh channel set before apply"),
                    ));
                }
            }
        }
    }
}

fn get_or_create_replica(
    shared: &Arc<Shared>,
    key: &(String, u32),
) -> Result<Arc<Replica>, BrokerError> {
    if let Some(r) = shared.replicas.read().unwrap().get(key) {
        return Ok(r.clone());
    }
    let dir = shared.config.data_dir.join(format!("{}-{}", key.0, key.1));
    let (handle, join) = partition::spawn(&dir, shared.config.log.clone())?;
    shared.threads.lock().unwrap().push(join);
    let replica = Replica::new(key.0.clone(), key.1, handle);
    shared
        .replicas
        .write()
        .unwrap()
        .insert(key.clone(), replica.clone());
    Ok(replica)
}

/// Leader-side ISR upkeep: propose shrinking out silent followers and
/// re-adding caught-up ones. The controller owns the decision.
async fn isr_maintenance(shared: &Arc<Shared>, conn: &Connection) {
    let self_id = shared.config.broker_id;
    let lag = Duration::from_millis(shared.config.replica_lag_ms);
    let replicas: Vec<Arc<Replica>> = shared.replicas.read().unwrap().values().cloned().collect();
    for replica in replicas {
        let proposal = {
            let st = replica.state.lock().unwrap();
            if !st.is_leader {
                continue;
            }
            let now = Instant::now();
            let mut desired: Vec<u32> = vec![self_id];
            for follower in st.replicas.iter().filter(|id| **id != self_id) {
                // One rule for shrink AND expand: in the ISR iff caught up
                // within the lag window. A follower that never fetched gets
                // the window measured from when this leader took over.
                let (_, last_caught_up) = st
                    .match_offsets
                    .get(follower)
                    .copied()
                    .unwrap_or((0, st.leader_since));
                if now.duration_since(last_caught_up) <= lag {
                    desired.push(*follower);
                }
            }
            desired.sort_unstable();
            let mut current = st.isr.clone();
            current.sort_unstable();
            if desired == current {
                None
            } else {
                Some((st.epoch, desired))
            }
        };
        let Some((epoch, desired)) = proposal else {
            continue;
        };
        match conn
            .call(&Request::AlterIsr {
                topic: replica.topic.clone(),
                partition: replica.partition,
                leader_epoch: epoch,
                isr: desired.clone(),
            })
            .await
        {
            Ok(Response::AlterIsr {
                error: ErrorCode::None,
                ..
            }) => {
                {
                    let mut st = replica.state.lock().unwrap();
                    if st.is_leader && st.epoch == epoch {
                        st.isr = desired;
                    }
                }
                replica.advance_hwm(self_id);
            }
            Ok(Response::AlterIsr { .. }) => shared.ping_refresh(),
            _ => {}
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
            if shared.is_clustered() {
                let shared = shared.clone();
                let out = out.clone();
                tokio::spawn(async move {
                    let conn = shared.controller_conn.read().unwrap().clone();
                    let resp = match conn {
                        None => Response::CreateTopic {
                            error: ErrorCode::Storage,
                        },
                        Some(conn) => match conn
                            .call(&Request::CreateTopic {
                                topic,
                                partitions,
                                replication_factor,
                            })
                            .await
                        {
                            Ok(resp @ Response::CreateTopic { .. }) => resp,
                            _ => Response::CreateTopic {
                                error: ErrorCode::Storage,
                            },
                        },
                    };
                    shared.ping_refresh();
                    respond(&out, &resp, corr);
                });
                return;
            }
            // Standalone broker cannot host replicas.
            let error = if replication_factor > 1 {
                ErrorCode::Malformed
            } else {
                shared.create_topic(&topic, partitions)
            };
            respond(out, &Response::CreateTopic { error }, corr);
        }
        Request::Metadata => {
            let cluster = if shared.is_clustered() {
                shared.cluster_meta.read().unwrap().clone()
            } else {
                shared.metadata()
            };
            respond(
                out,
                &Response::Metadata {
                    error: ErrorCode::None,
                    cluster,
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
            let produce_error = |error| Response::Produce {
                error,
                base_offset: 0,
                count: 0,
            };
            if shared.is_clustered() {
                let Some(replica) = shared.replica(&topic, partition) else {
                    if acks != Acks::None {
                        let known = shared
                            .cluster_meta
                            .read()
                            .unwrap()
                            .topics
                            .iter()
                            .any(|t| t.name == topic);
                        let code = if known {
                            ErrorCode::NotLeader
                        } else {
                            ErrorCode::UnknownTopicOrPartition
                        };
                        respond(out, &produce_error(code), corr);
                    }
                    return;
                };
                let (is_leader, epoch, isr_len) = {
                    let st = replica.state.lock().unwrap();
                    (st.is_leader, st.epoch, st.isr.len())
                };
                let precheck = if !is_leader {
                    Some(ErrorCode::NotLeader)
                } else if leader_epoch != 0 && leader_epoch != epoch {
                    Some(ErrorCode::FencedEpoch)
                } else if acks == Acks::All
                    && (isr_len as u32) < shared.config.min_insync_replicas
                {
                    Some(ErrorCode::NotEnoughReplicas)
                } else {
                    None
                };
                if let Some(code) = precheck {
                    if acks != Acks::None {
                        respond(out, &produce_error(code), corr);
                    }
                    return;
                }
                let records: Vec<_> = records.into_iter().map(|r| (r.key, r.value)).collect();
                if acks == Acks::None {
                    replica.handle.append_no_reply(records);
                    return;
                }
                if acks != Acks::All {
                    let reply = replica.handle.append_start(records, acks);
                    let out = out.clone();
                    tokio::spawn(async move {
                        let resp = match reply.await.unwrap_or(Err(ErrorCode::Storage)) {
                            Ok((base_offset, count)) => Response::Produce {
                                error: ErrorCode::None,
                                base_offset,
                                count,
                            },
                            Err(error) => produce_error(error),
                        };
                        respond(&out, &resp, corr);
                    });
                    return;
                }
                // acks=all: append now (order preserved), ack when the HWM
                // covers the batch — i.e. every ISR member has it.
                let reply = replica.handle.append_start(records, Acks::Written);
                let out = out.clone();
                let self_id = shared.config.broker_id;
                tokio::spawn(async move {
                    let (base, count) = match reply.await.unwrap_or(Err(ErrorCode::Storage)) {
                        Ok(ok) => ok,
                        Err(error) => {
                            respond(&out, &produce_error(error), corr);
                            return;
                        }
                    };
                    if count == 0 {
                        respond(
                            &out,
                            &Response::Produce {
                                error: ErrorCode::None,
                                base_offset: base,
                                count,
                            },
                            corr,
                        );
                        return;
                    }
                    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                    {
                        let mut st = replica.state.lock().unwrap();
                        st.pending_all.push((base + count as u64 - 1, base, count, ack_tx));
                    }
                    replica.advance_hwm(self_id);
                    let resp = match tokio::time::timeout(Duration::from_secs(15), ack_rx).await
                    {
                        Ok(Ok(Ok((base_offset, count)))) => Response::Produce {
                            error: ErrorCode::None,
                            base_offset,
                            count,
                        },
                        Ok(Ok(Err(error))) => produce_error(error),
                        // Dropped or timed out: replication never covered it.
                        _ => produce_error(ErrorCode::NotEnoughReplicas),
                    };
                    respond(&out, &resp, corr);
                });
                return;
            }
            if leader_epoch != 0 {
                if acks != Acks::None {
                    respond(out, &produce_error(ErrorCode::FencedEpoch), corr);
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
            let fetch_error = |error| Response::Fetch {
                error,
                log_start: 0,
                next_offset: 0,
                records: Vec::new(),
            };
            if shared.is_clustered() {
                let Some(replica) = shared.replica(&topic, partition) else {
                    respond(out, &fetch_error(ErrorCode::NotLeader), corr);
                    return;
                };
                if !replica.state.lock().unwrap().is_leader {
                    respond(out, &fetch_error(ErrorCode::NotLeader), corr);
                    return;
                }
                let out = out.clone();
                tokio::spawn(async move {
                    let resp = handle_fetch_hwm(replica, offset, max_bytes, max_wait_ms).await;
                    respond(&out, &resp, corr);
                });
                return;
            }
            let Some(handle) = shared.partition(&topic, partition) else {
                respond(out, &fetch_error(ErrorCode::UnknownTopicOrPartition), corr);
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
        Request::ReplicaFetch {
            topic,
            partition,
            follower_id,
            leader_epoch,
            offset,
            max_bytes,
            max_wait_ms,
        } => {
            let replica_error = |error| Response::ReplicaFetch {
                error,
                leader_epoch: 0,
                log_end: 0,
                high_watermark: 0,
                records: Vec::new(),
            };
            let Some(replica) = shared.replica(&topic, partition) else {
                respond(out, &replica_error(ErrorCode::NotLeader), corr);
                return;
            };
            let log_end = *replica.handle.next_offset.borrow();
            let accepted = {
                let mut st = replica.state.lock().unwrap();
                if !st.is_leader {
                    Some(ErrorCode::NotLeader)
                } else if leader_epoch != st.epoch {
                    Some(ErrorCode::FencedEpoch)
                } else {
                    // A fetch at `offset` proves the follower has everything
                    // before it — that is the replication progress signal.
                    // The caught-up clock only advances when the fetch
                    // reaches the log end; a fetch that is merely *recent*
                    // must not count as keeping up.
                    let caught_up = if offset >= log_end {
                        Instant::now()
                    } else {
                        st.match_offsets
                            .get(&follower_id)
                            .map(|&(_, t)| t)
                            .unwrap_or(st.leader_since)
                    };
                    st.match_offsets.insert(follower_id, (offset, caught_up));
                    None
                }
            };
            if let Some(code) = accepted {
                respond(out, &replica_error(code), corr);
                return;
            }
            let self_id = shared.config.broker_id;
            replica.advance_hwm(self_id);
            let out = out.clone();
            tokio::spawn(async move {
                let resp =
                    handle_replica_fetch(replica, leader_epoch, offset, max_bytes, max_wait_ms)
                        .await;
                respond(&out, &resp, corr);
            });
        }
        Request::EpochCheck {
            topic,
            partition,
            epoch,
        } => {
            let Some(replica) = shared.replica(&topic, partition) else {
                respond(
                    out,
                    &Response::EpochCheck {
                        error: ErrorCode::NotLeader,
                        end_offset: 0,
                    },
                    corr,
                );
                return;
            };
            if !replica.state.lock().unwrap().is_leader {
                respond(
                    out,
                    &Response::EpochCheck {
                        error: ErrorCode::NotLeader,
                        end_offset: 0,
                    },
                    corr,
                );
                return;
            }
            let out = out.clone();
            tokio::spawn(async move {
                let resp = match replica.handle.epoch_end_for(epoch).await {
                    Ok(end_offset) => Response::EpochCheck {
                        error: ErrorCode::None,
                        end_offset,
                    },
                    Err(error) => Response::EpochCheck {
                        error,
                        end_offset: 0,
                    },
                };
                respond(&out, &resp, corr);
            });
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

/// Consumer fetch on a replicated partition: bounded by the high-water mark.
/// Records past the HWM exist but are not yet replication-committed; handing
/// them out would let a consumer see data a failover can erase.
async fn handle_fetch_hwm(
    replica: Arc<Replica>,
    offset: u64,
    max_bytes: u32,
    max_wait_ms: u32,
) -> Response {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(max_wait_ms as u64);
    let mut hwm_rx = replica.hwm_rx.clone();
    loop {
        let hwm = *hwm_rx.borrow();
        let log_end = *replica.handle.next_offset.borrow();
        if offset > log_end {
            return Response::Fetch {
                error: ErrorCode::OffsetOutOfRange,
                log_start: 0,
                next_offset: hwm,
                records: Vec::new(),
            };
        }
        if offset < hwm {
            match replica.handle.read(offset, max_bytes as u64).await {
                Ok(ok) => {
                    let mut records: Vec<FetchedRecord> = ok
                        .records
                        .into_iter()
                        .take_while(|r| r.offset < hwm)
                        .map(|r| FetchedRecord {
                            offset: r.offset,
                            timestamp_ms: r.timestamp_ms,
                            key: r.key,
                            value: r.value,
                        })
                        .collect();
                    if records.is_empty() {
                        // Between our HWM read and the log read the log
                        // truncated or raced; report empty rather than spin.
                        records = Vec::new();
                    }
                    return Response::Fetch {
                        error: ErrorCode::None,
                        log_start: ok.log_start,
                        next_offset: hwm,
                        records,
                    };
                }
                Err(error) => {
                    return Response::Fetch {
                        error,
                        log_start: 0,
                        next_offset: hwm,
                        records: Vec::new(),
                    };
                }
            }
        }
        let woke = tokio::time::timeout_at(deadline, hwm_rx.wait_for(|&h| h > offset)).await;
        match woke {
            Ok(Ok(_)) => continue,
            _ => {
                return Response::Fetch {
                    error: ErrorCode::None,
                    log_start: 0,
                    next_offset: *replica.hwm_rx.borrow(),
                    records: Vec::new(),
                };
            }
        }
    }
}

/// Follower fetch: reads to the log end (past the HWM — replication is how
/// records BECOME committed), long-polling on the log end watch.
async fn handle_replica_fetch(
    replica: Arc<Replica>,
    leader_epoch: u64,
    offset: u64,
    max_bytes: u32,
    max_wait_ms: u32,
) -> Response {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(max_wait_ms as u64);
    let mut next_offset = replica.handle.next_offset.clone();
    let result = loop {
        match replica.handle.read(offset, max_bytes as u64).await {
            Ok(ok) if ok.records.is_empty() && tokio::time::Instant::now() < deadline => {
                let woke =
                    tokio::time::timeout_at(deadline, next_offset.wait_for(|&n| n > offset)).await;
                match woke {
                    Ok(Ok(_)) => continue,
                    _ => break Ok(ok),
                }
            }
            other => break other,
        }
    };
    match result {
        Ok(ok) => Response::ReplicaFetch {
            error: ErrorCode::None,
            leader_epoch,
            log_end: ok.next_offset,
            high_watermark: *replica.hwm_rx.borrow(),
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
        Err(error) => Response::ReplicaFetch {
            error,
            leader_epoch,
            log_end: 0,
            high_watermark: 0,
            records: Vec::new(),
        },
    }
}
