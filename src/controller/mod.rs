//! The controller: the cluster's metadata brain, deliberately boring.
//!
//! Single and static (the documented SPOF simplification — real systems make
//! this a Raft group, which is exactly where consensus belongs and the only
//! place it's paid for). It tracks broker liveness by heartbeat, owns the
//! (replicas, leader, leader_epoch, ISR) tuple per partition, elects leaders
//! from the ISR only (no unclean elections), and persists every change as an
//! fsynced, atomically-renamed snapshot.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use crate::proto::{
    self, ClusterMeta, ErrorCode, PartitionMeta, Request, Response, TopicMeta, wire,
};
use crate::storage::fsync_file;

const SNAPSHOT_MAGIC: u32 = 0x524C_4354; // "RLCT"

#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub state_file: PathBuf,
    /// A broker missing heartbeats for this long is declared dead.
    pub session_timeout: Duration,
}

struct BrokerState {
    addr: String,
    alive: bool,
    last_heartbeat: Instant,
}

#[derive(Clone)]
struct PartitionState {
    leader: i32,
    leader_epoch: u64,
    replicas: Vec<u32>,
    isr: Vec<u32>,
}

struct State {
    version: u64,
    brokers: HashMap<u32, BrokerState>,
    topics: HashMap<String, Vec<PartitionState>>,
}

pub struct ControllerHandle {
    pub addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    accept_task: JoinHandle<()>,
    sweep_task: JoinHandle<()>,
}

impl ControllerHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.accept_task.await;
        let _ = self.sweep_task.await;
    }
}

pub struct Controller;

struct Shared {
    config: ControllerConfig,
    state: Mutex<State>,
}

impl Controller {
    pub async fn start(
        listen: &str,
        config: ControllerConfig,
    ) -> std::io::Result<ControllerHandle> {
        let state = load_snapshot(&config.state_file)?;
        let shared = Arc::new(Shared {
            config,
            state: Mutex::new(state),
        });
        let listener = TcpListener::bind(listen).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let sweep_shared = shared.clone();
        let mut sweep_shutdown = shutdown_rx.clone();
        let sweep_task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(200));
            loop {
                tokio::select! {
                    _ = tick.tick() => sweep_shared.sweep_liveness(),
                    _ = sweep_shutdown.changed() => break,
                }
            }
        });

        let accept_shared = shared.clone();
        let accept_task = tokio::spawn(async move {
            let mut conns = JoinSet::new();
            let mut shutdown = shutdown_rx.clone();
            loop {
                tokio::select! {
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => {
                            conns.spawn(serve_connection(
                                accept_shared.clone(),
                                stream,
                                shutdown_rx.clone(),
                            ));
                        }
                        Err(e) => {
                            eprintln!("controller accept failed: {e}");
                            break;
                        }
                    },
                    _ = shutdown.changed() => break,
                }
            }
            while conns.join_next().await.is_some() {}
        });

        Ok(ControllerHandle {
            addr,
            shutdown_tx,
            accept_task,
            sweep_task,
        })
    }
}

async fn serve_connection(
    shared: Arc<Shared>,
    stream: TcpStream,
    mut shutdown: watch::Receiver<bool>,
) {
    let _ = stream.set_nodelay(true);
    let (mut rd, mut wr) = stream.into_split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
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
                _ => break,
            },
            _ = shutdown.changed() => break,
        };
        match proto::decode_request(&frame) {
            Ok((corr, req)) => {
                let resp = shared.handle(req);
                let _ = out_tx.send(proto::encode_response(&resp, corr));
            }
            Err(e) => {
                eprintln!("controller: undecodable frame ({e})");
                break;
            }
        }
    }
    drop(out_tx);
    let _ = writer.await;
}

impl Shared {
    fn handle(&self, req: Request) -> Response {
        match req {
            Request::RegisterBroker { broker_id, addr } => {
                let version = self.mutate(|state| {
                    let entry = state.brokers.entry(broker_id).or_insert(BrokerState {
                        addr: addr.clone(),
                        alive: false,
                        last_heartbeat: Instant::now(),
                    });
                    let changed = !entry.alive || entry.addr != addr;
                    entry.addr = addr;
                    entry.alive = true;
                    entry.last_heartbeat = Instant::now();
                    let elected = elect_where_needed(state);
                    changed || elected
                });
                Response::RegisterBroker {
                    error: ErrorCode::None,
                    metadata_version: version,
                }
            }
            Request::BrokerHeartbeat { broker_id, .. } => {
                let mut known = false;
                let version = self.mutate(|state| {
                    let Some(b) = state.brokers.get_mut(&broker_id) else {
                        return false;
                    };
                    known = true;
                    let was_dead = !b.alive;
                    b.alive = true;
                    b.last_heartbeat = Instant::now();
                    if was_dead { elect_where_needed(state) } else { false }
                });
                Response::BrokerHeartbeat {
                    error: if known {
                        ErrorCode::None
                    } else {
                        ErrorCode::UnknownMember
                    },
                    metadata_version: version,
                }
            }
            Request::ControllerMetadata => Response::ControllerMetadata {
                error: ErrorCode::None,
                cluster: self.cluster_meta(),
            },
            Request::CreateTopic {
                topic,
                partitions,
                replication_factor,
            } => {
                let rf = replication_factor.max(1);
                let mut error = ErrorCode::None;
                self.mutate(|state| {
                    if state.topics.contains_key(&topic) {
                        error = ErrorCode::TopicExists;
                        return false;
                    }
                    let mut alive: Vec<u32> = state
                        .brokers
                        .iter()
                        .filter(|(_, b)| b.alive)
                        .map(|(id, _)| *id)
                        .collect();
                    alive.sort_unstable();
                    if partitions == 0 || partitions > 1024 || (rf as usize) > alive.len() {
                        error = ErrorCode::Malformed;
                        return false;
                    }
                    let parts = (0..partitions)
                        .map(|p| {
                            let replicas: Vec<u32> = (0..rf)
                                .map(|i| alive[((p + i) as usize) % alive.len()])
                                .collect();
                            PartitionState {
                                leader: replicas[0] as i32,
                                leader_epoch: 1,
                                isr: replicas.clone(),
                                replicas,
                            }
                        })
                        .collect();
                    state.topics.insert(topic.clone(), parts);
                    true
                });
                Response::CreateTopic { error }
            }
            Request::AlterIsr {
                topic,
                partition,
                leader_epoch,
                isr,
            } => {
                let mut error = ErrorCode::None;
                let version = self.mutate(|state| {
                    let Some(p) = state
                        .topics
                        .get_mut(&topic)
                        .and_then(|ps| ps.get_mut(partition as usize))
                    else {
                        error = ErrorCode::UnknownTopicOrPartition;
                        return false;
                    };
                    if leader_epoch != p.leader_epoch {
                        error = ErrorCode::FencedEpoch;
                        return false;
                    }
                    let leader = p.leader as u32;
                    if isr.is_empty()
                        || !isr.contains(&leader)
                        || !isr.iter().all(|id| p.replicas.contains(id))
                    {
                        error = ErrorCode::Malformed;
                        return false;
                    }
                    if p.isr == isr {
                        return false;
                    }
                    p.isr = isr;
                    true
                });
                Response::AlterIsr {
                    error,
                    metadata_version: version,
                }
            }
            // Data-plane and group messages don't belong here.
            _ => Response::ControllerMetadata {
                error: ErrorCode::Malformed,
                cluster: ClusterMeta::default(),
            },
        }
    }

    /// Runs a mutation; if it reports a change, bumps the version and
    /// persists the snapshot before releasing the lock. Returns the version.
    fn mutate(&self, f: impl FnOnce(&mut State) -> bool) -> u64 {
        let mut state = self.state.lock().unwrap();
        if f(&mut state) {
            state.version += 1;
            if let Err(e) = persist_snapshot(&self.config.state_file, &state) {
                eprintln!("controller: snapshot persist failed: {e}");
            }
        }
        state.version
    }

    fn sweep_liveness(&self) {
        let timeout = self.config.session_timeout;
        self.mutate(|state| {
            let now = Instant::now();
            let mut changed = false;
            for b in state.brokers.values_mut() {
                if b.alive && now.duration_since(b.last_heartbeat) > timeout {
                    b.alive = false;
                    changed = true;
                }
            }
            if changed {
                elect_where_needed(state);
            }
            changed
        });
    }

    fn cluster_meta(&self) -> ClusterMeta {
        let state = self.state.lock().unwrap();
        let mut brokers: Vec<(u32, String)> = state
            .brokers
            .iter()
            .map(|(id, b)| (*id, b.addr.clone()))
            .collect();
        brokers.sort();
        let mut topics: Vec<TopicMeta> = state
            .topics
            .iter()
            .map(|(name, parts)| TopicMeta {
                name: name.clone(),
                partitions: parts
                    .iter()
                    .enumerate()
                    .map(|(i, p)| PartitionMeta {
                        partition: i as u32,
                        leader: p.leader,
                        leader_epoch: p.leader_epoch,
                        replicas: p.replicas.clone(),
                        isr: p.isr.clone(),
                    })
                    .collect(),
            })
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        ClusterMeta {
            version: state.version,
            brokers,
            topics,
        }
    }
}

/// Elects a leader for every partition whose leader is dead or missing, from
/// the ISR only (minus dead members). Returns whether anything changed.
fn elect_where_needed(state: &mut State) -> bool {
    let alive: std::collections::HashSet<u32> = state
        .brokers
        .iter()
        .filter(|(_, b)| b.alive)
        .map(|(id, _)| *id)
        .collect();
    let mut changed = false;
    for parts in state.topics.values_mut() {
        for p in parts.iter_mut() {
            let leader_alive = p.leader >= 0 && alive.contains(&(p.leader as u32));
            if leader_alive {
                continue;
            }
            let candidate = p.isr.iter().find(|id| alive.contains(id)).copied();
            match candidate {
                Some(new_leader) => {
                    p.leader = new_leader as i32;
                    p.leader_epoch += 1;
                    p.isr.retain(|id| alive.contains(id));
                    changed = true;
                }
                None => {
                    if p.leader != -1 {
                        // No unclean election: offline until an ISR member
                        // returns. Availability lost, acked data not.
                        p.leader = -1;
                        changed = true;
                    }
                }
            }
        }
    }
    changed
}

fn persist_snapshot(path: &PathBuf, state: &State) -> std::io::Result<()> {
    let mut buf = Vec::new();
    wire::put_u32(&mut buf, SNAPSHOT_MAGIC);
    wire::put_u64(&mut buf, state.version);
    wire::put_u32(&mut buf, state.brokers.len() as u32);
    let mut broker_ids: Vec<&u32> = state.brokers.keys().collect();
    broker_ids.sort();
    for id in broker_ids {
        wire::put_u32(&mut buf, *id);
        wire::put_str(&mut buf, &state.brokers[id].addr);
    }
    wire::put_u32(&mut buf, state.topics.len() as u32);
    let mut names: Vec<&String> = state.topics.keys().collect();
    names.sort();
    for name in names {
        wire::put_str(&mut buf, name);
        let parts = &state.topics[name];
        wire::put_u32(&mut buf, parts.len() as u32);
        for p in parts {
            wire::put_i32(&mut buf, p.leader);
            wire::put_u64(&mut buf, p.leader_epoch);
            wire::put_u32(&mut buf, p.replicas.len() as u32);
            for r in &p.replicas {
                wire::put_u32(&mut buf, *r);
            }
            wire::put_u32(&mut buf, p.isr.len() as u32);
            for r in &p.isr {
                wire::put_u32(&mut buf, *r);
            }
        }
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        use std::io::Write;
        file.write_all(&buf)?;
        fsync_file(&file)?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn load_snapshot(path: &PathBuf) -> std::io::Result<State> {
    let empty = State {
        version: 0,
        brokers: HashMap::new(),
        topics: HashMap::new(),
    };
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(empty),
        Err(e) => return Err(e),
    };
    let bad = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_string());
    let mut r = wire::Reader::new(&bytes);
    if r.u32().map_err(|_| bad("truncated"))? != SNAPSHOT_MAGIC {
        return Err(bad("bad snapshot magic"));
    }
    let mut parse = || -> Result<State, proto::ProtoError> {
        let version = r.u64()?;
        let broker_count = r.u32()?;
        let mut brokers = HashMap::new();
        for _ in 0..broker_count {
            let id = r.u32()?;
            let addr = r.string()?;
            brokers.insert(
                id,
                BrokerState {
                    addr,
                    // Nobody is trusted as alive until they heartbeat again.
                    alive: false,
                    last_heartbeat: Instant::now(),
                },
            );
        }
        let topic_count = r.u32()?;
        let mut topics = HashMap::new();
        for _ in 0..topic_count {
            let name = r.string()?;
            let part_count = r.u32()?;
            let mut parts = Vec::new();
            for _ in 0..part_count {
                let leader = r.i32()?;
                let leader_epoch = r.u64()?;
                let rc = r.u32()?;
                let mut replicas = Vec::new();
                for _ in 0..rc {
                    replicas.push(r.u32()?);
                }
                let ic = r.u32()?;
                let mut isr = Vec::new();
                for _ in 0..ic {
                    isr.push(r.u32()?);
                }
                parts.push(PartitionState {
                    leader,
                    leader_epoch,
                    replicas,
                    isr,
                });
            }
            topics.insert(name, parts);
        }
        r.expect_end()?;
        Ok(State {
            version,
            brokers,
            topics,
        })
    };
    parse().map_err(|e| bad(&format!("malformed snapshot: {e}")))
}
