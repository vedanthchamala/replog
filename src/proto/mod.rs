//! Wire protocol: length-prefixed frames carrying hand-rolled little-endian
//! messages, shared verbatim by broker and clients.
//!
//! Frame layout (both directions):
//!
//! ```text
//! u32  len             length of everything after this field
//! u8   version         protocol version (currently 1)
//! u8   msg_type        request type; responses set the high bit (| 0x80)
//! u32  correlation_id  chosen by the client, echoed by the broker
//!      body            per message type
//! ```
//!
//! Correlation IDs make pipelining safe: responses may arrive out of order
//! (long-poll fetches), and the client routes each to its waiting caller.

pub(crate) mod wire;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wire::Reader;

pub const VERSION: u8 = 1;
pub const MAX_FRAME_BYTES: u32 = 32 * 1024 * 1024;
const RESPONSE_BIT: u8 = 0x80;

const MSG_CREATE_TOPIC: u8 = 1;
const MSG_METADATA: u8 = 2;
const MSG_PRODUCE: u8 = 3;
const MSG_FETCH: u8 = 4;
const MSG_COMMIT_OFFSET: u8 = 5;
const MSG_FETCH_OFFSET: u8 = 6;
const MSG_JOIN_GROUP: u8 = 7;
const MSG_HEARTBEAT: u8 = 8;
const MSG_LEAVE_GROUP: u8 = 9;
const MSG_REPLICA_FETCH: u8 = 10;
const MSG_EPOCH_CHECK: u8 = 11;
const MSG_REGISTER_BROKER: u8 = 12;
const MSG_BROKER_HEARTBEAT: u8 = 13;
const MSG_ALTER_ISR: u8 = 14;
const MSG_CONTROLLER_METADATA: u8 = 15;

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("frame truncated")]
    Truncated,
    #[error("malformed frame: {0}")]
    Malformed(String),
    #[error("unsupported protocol version {0}")]
    BadVersion(u8),
    #[error("unknown message type {0:#x}")]
    UnknownMsgType(u8),
}

pub type Result<T> = std::result::Result<T, ProtoError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    None = 0,
    UnknownTopicOrPartition = 1,
    OffsetOutOfRange = 2,
    Storage = 3,
    Malformed = 4,
    TopicExists = 5,
    UnknownMember = 6,
    /// The caller's group generation is behind: a rebalance happened. The
    /// only correct reaction is to rejoin and pick up the new assignment.
    StaleGeneration = 7,
    /// This broker does not lead the partition; refresh metadata and retry.
    NotLeader = 8,
    /// The caller's leader epoch is behind the partition's current epoch.
    FencedEpoch = 9,
    /// acks=all rejected: the ISR is smaller than min_insync_replicas.
    NotEnoughReplicas = 10,
    /// The partition has no live leader (no unclean election).
    Offline = 11,
}

impl ErrorCode {
    fn from_u16(v: u16) -> Result<Self> {
        Ok(match v {
            0 => Self::None,
            1 => Self::UnknownTopicOrPartition,
            2 => Self::OffsetOutOfRange,
            3 => Self::Storage,
            4 => Self::Malformed,
            5 => Self::TopicExists,
            6 => Self::UnknownMember,
            7 => Self::StaleGeneration,
            8 => Self::NotLeader,
            9 => Self::FencedEpoch,
            10 => Self::NotEnoughReplicas,
            11 => Self::Offline,
            _ => return Err(ProtoError::Malformed(format!("unknown error code {v}"))),
        })
    }
}

/// When the broker acknowledges a produce.
///
/// `Durable` is the strongest single-broker contract: the ack is held until the
/// covering flush completes. A broker running `FsyncPolicy::Os` never flushes,
/// so there it degrades to `Written` — acks cannot strengthen the broker's
/// configured durability, only choose how much of it to wait for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Acks {
    /// Fire-and-forget: the broker sends no response frame at all.
    None = 0,
    /// Acked once appended to the log (page cache).
    Written = 1,
    /// Acked once the covering flush has reached stable media.
    Durable = 2,
    /// Acked once every in-sync replica has the record (HWM has passed it).
    /// Durability by replication — followers ack on write, not flush.
    All = 3,
}

impl Acks {
    fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => Self::None,
            1 => Self::Written,
            2 => Self::Durable,
            3 => Self::All,
            _ => return Err(ProtoError::Malformed(format!("unknown acks value {v}"))),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMeta {
    pub partition: u32,
    /// Leading broker id, or -1 when the partition is offline.
    pub leader: i32,
    pub leader_epoch: u64,
    pub replicas: Vec<u32>,
    pub isr: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMeta {
    pub name: String,
    pub partitions: Vec<PartitionMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterMeta {
    pub version: u64,
    /// Client-facing address per broker.
    pub brokers: Vec<(u32, String)>,
    /// Inter-broker address per broker (follower fetch, epoch checks). Equal
    /// to the client address unless the broker advertises a separate one —
    /// containers with a private peers network do.
    pub peers: Vec<(u32, String)>,
    pub topics: Vec<TopicMeta>,
}

fn put_cluster_meta(out: &mut Vec<u8>, meta: &ClusterMeta) {
    wire::put_u64(out, meta.version);
    wire::put_u32(out, meta.brokers.len() as u32);
    for (id, addr) in &meta.brokers {
        wire::put_u32(out, *id);
        wire::put_str(out, addr);
    }
    wire::put_u32(out, meta.peers.len() as u32);
    for (id, addr) in &meta.peers {
        wire::put_u32(out, *id);
        wire::put_str(out, addr);
    }
    wire::put_u32(out, meta.topics.len() as u32);
    for topic in &meta.topics {
        wire::put_str(out, &topic.name);
        wire::put_u32(out, topic.partitions.len() as u32);
        for p in &topic.partitions {
            wire::put_u32(out, p.partition);
            wire::put_i32(out, p.leader);
            wire::put_u64(out, p.leader_epoch);
            wire::put_u32(out, p.replicas.len() as u32);
            for r in &p.replicas {
                wire::put_u32(out, *r);
            }
            wire::put_u32(out, p.isr.len() as u32);
            for r in &p.isr {
                wire::put_u32(out, *r);
            }
        }
    }
}

fn read_cluster_meta(r: &mut wire::Reader<'_>) -> Result<ClusterMeta> {
    let version = r.u64()?;
    let broker_count = r.u32()?;
    let mut brokers = Vec::new();
    for _ in 0..broker_count {
        brokers.push((r.u32()?, r.string()?));
    }
    let peer_count = r.u32()?;
    let mut peers = Vec::new();
    for _ in 0..peer_count {
        peers.push((r.u32()?, r.string()?));
    }
    let topic_count = r.u32()?;
    let mut topics = Vec::new();
    for _ in 0..topic_count {
        let name = r.string()?;
        let partition_count = r.u32()?;
        let mut partitions = Vec::new();
        for _ in 0..partition_count {
            let partition = r.u32()?;
            let leader = r.i32()?;
            let leader_epoch = r.u64()?;
            let replica_count = r.u32()?;
            let mut replicas = Vec::new();
            for _ in 0..replica_count {
                replicas.push(r.u32()?);
            }
            let isr_count = r.u32()?;
            let mut isr = Vec::new();
            for _ in 0..isr_count {
                isr.push(r.u32()?);
            }
            partitions.push(PartitionMeta {
                partition,
                leader,
                leader_epoch,
                replicas,
                isr,
            });
        }
        topics.push(TopicMeta { name, partitions });
    }
    Ok(ClusterMeta {
        version,
        brokers,
        peers,
        topics,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRecord {
    pub offset: u64,
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    CreateTopic {
        topic: String,
        partitions: u32,
        /// 0 is treated as 1 (standalone-era clients).
        replication_factor: u32,
    },
    Metadata,
    Produce {
        topic: String,
        partition: u32,
        acks: Acks,
        /// The leader epoch the client believes; 0 = don't care. A stale
        /// nonzero epoch is rejected with FencedEpoch.
        leader_epoch: u64,
        records: Vec<ProduceRecord>,
    },
    Fetch {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    },
    ReplicaFetch {
        topic: String,
        partition: u32,
        follower_id: u32,
        leader_epoch: u64,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    },
    /// "Where did `epoch` end in your history?" — asked by a new follower
    /// before fetching, so it can truncate a divergent suffix.
    EpochCheck {
        topic: String,
        partition: u32,
        epoch: u64,
    },
    RegisterBroker {
        broker_id: u32,
        /// Where clients should dial this broker.
        addr: String,
        /// Where other brokers should dial it (same as `addr` by default).
        peer_addr: String,
    },
    BrokerHeartbeat {
        broker_id: u32,
        metadata_version: u64,
    },
    AlterIsr {
        topic: String,
        partition: u32,
        leader_epoch: u64,
        isr: Vec<u32>,
    },
    ControllerMetadata,
    CommitOffset {
        group: String,
        topic: String,
        partition: u32,
        offset: u64,
        /// Empty = unfenced standalone commit. Otherwise the commit is
        /// rejected unless (member_id, generation) match the group's current
        /// membership — zombie fencing.
        member_id: String,
        generation: u64,
    },
    FetchOffset {
        group: String,
        topic: String,
        partition: u32,
    },
    JoinGroup {
        group: String,
        /// Empty = new member; the broker assigns an id.
        member_id: String,
        session_timeout_ms: u32,
        topics: Vec<String>,
    },
    Heartbeat {
        group: String,
        member_id: String,
        generation: u64,
    },
    LeaveGroup {
        group: String,
        member_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    CreateTopic {
        error: ErrorCode,
    },
    Metadata {
        error: ErrorCode,
        cluster: ClusterMeta,
    },
    Produce {
        error: ErrorCode,
        base_offset: u64,
        count: u32,
    },
    Fetch {
        error: ErrorCode,
        log_start: u64,
        /// Consumers may read up to here (exclusive). With replication this
        /// is the high-water mark, not the log end.
        next_offset: u64,
        records: Vec<FetchedRecord>,
    },
    ReplicaFetch {
        error: ErrorCode,
        leader_epoch: u64,
        log_end: u64,
        high_watermark: u64,
        records: Vec<FetchedRecord>,
    },
    EpochCheck {
        error: ErrorCode,
        end_offset: u64,
    },
    RegisterBroker {
        error: ErrorCode,
        metadata_version: u64,
    },
    BrokerHeartbeat {
        error: ErrorCode,
        metadata_version: u64,
    },
    AlterIsr {
        error: ErrorCode,
        metadata_version: u64,
    },
    ControllerMetadata {
        error: ErrorCode,
        cluster: ClusterMeta,
    },
    CommitOffset {
        error: ErrorCode,
    },
    FetchOffset {
        error: ErrorCode,
        /// Committed offset, or -1 if the group has never committed.
        offset: i64,
    },
    JoinGroup {
        error: ErrorCode,
        member_id: String,
        generation: u64,
        assignment: Vec<(String, u32)>,
    },
    Heartbeat {
        error: ErrorCode,
    },
    LeaveGroup {
        error: ErrorCode,
    },
}

fn frame_header(out: &mut Vec<u8>, msg_type: u8, correlation_id: u32) {
    out.extend_from_slice(&[0u8; 4]);
    wire::put_u8(out, VERSION);
    wire::put_u8(out, msg_type);
    wire::put_u32(out, correlation_id);
}

fn finish_frame(mut out: Vec<u8>) -> Vec<u8> {
    let len = (out.len() - 4) as u32;
    out[0..4].copy_from_slice(&len.to_le_bytes());
    out
}

pub fn encode_request(req: &Request, correlation_id: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    match req {
        Request::CreateTopic {
            topic,
            partitions,
            replication_factor,
        } => {
            frame_header(&mut out, MSG_CREATE_TOPIC, correlation_id);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partitions);
            wire::put_u32(&mut out, *replication_factor);
        }
        Request::Metadata => {
            frame_header(&mut out, MSG_METADATA, correlation_id);
        }
        Request::Produce {
            topic,
            partition,
            acks,
            leader_epoch,
            records,
        } => {
            frame_header(&mut out, MSG_PRODUCE, correlation_id);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partition);
            wire::put_u8(&mut out, *acks as u8);
            wire::put_u64(&mut out, *leader_epoch);
            wire::put_u32(&mut out, records.len() as u32);
            for r in records {
                wire::put_opt_bytes(&mut out, r.key.as_deref());
                wire::put_bytes(&mut out, &r.value);
            }
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
            frame_header(&mut out, MSG_REPLICA_FETCH, correlation_id);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partition);
            wire::put_u32(&mut out, *follower_id);
            wire::put_u64(&mut out, *leader_epoch);
            wire::put_u64(&mut out, *offset);
            wire::put_u32(&mut out, *max_bytes);
            wire::put_u32(&mut out, *max_wait_ms);
        }
        Request::EpochCheck {
            topic,
            partition,
            epoch,
        } => {
            frame_header(&mut out, MSG_EPOCH_CHECK, correlation_id);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partition);
            wire::put_u64(&mut out, *epoch);
        }
        Request::RegisterBroker {
            broker_id,
            addr,
            peer_addr,
        } => {
            frame_header(&mut out, MSG_REGISTER_BROKER, correlation_id);
            wire::put_u32(&mut out, *broker_id);
            wire::put_str(&mut out, addr);
            wire::put_str(&mut out, peer_addr);
        }
        Request::BrokerHeartbeat {
            broker_id,
            metadata_version,
        } => {
            frame_header(&mut out, MSG_BROKER_HEARTBEAT, correlation_id);
            wire::put_u32(&mut out, *broker_id);
            wire::put_u64(&mut out, *metadata_version);
        }
        Request::AlterIsr {
            topic,
            partition,
            leader_epoch,
            isr,
        } => {
            frame_header(&mut out, MSG_ALTER_ISR, correlation_id);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partition);
            wire::put_u64(&mut out, *leader_epoch);
            wire::put_u32(&mut out, isr.len() as u32);
            for id in isr {
                wire::put_u32(&mut out, *id);
            }
        }
        Request::ControllerMetadata => {
            frame_header(&mut out, MSG_CONTROLLER_METADATA, correlation_id);
        }
        Request::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
            max_wait_ms,
        } => {
            frame_header(&mut out, MSG_FETCH, correlation_id);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partition);
            wire::put_u64(&mut out, *offset);
            wire::put_u32(&mut out, *max_bytes);
            wire::put_u32(&mut out, *max_wait_ms);
        }
        Request::CommitOffset {
            group,
            topic,
            partition,
            offset,
            member_id,
            generation,
        } => {
            frame_header(&mut out, MSG_COMMIT_OFFSET, correlation_id);
            wire::put_str(&mut out, group);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partition);
            wire::put_u64(&mut out, *offset);
            wire::put_str(&mut out, member_id);
            wire::put_u64(&mut out, *generation);
        }
        Request::FetchOffset {
            group,
            topic,
            partition,
        } => {
            frame_header(&mut out, MSG_FETCH_OFFSET, correlation_id);
            wire::put_str(&mut out, group);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partition);
        }
        Request::JoinGroup {
            group,
            member_id,
            session_timeout_ms,
            topics,
        } => {
            frame_header(&mut out, MSG_JOIN_GROUP, correlation_id);
            wire::put_str(&mut out, group);
            wire::put_str(&mut out, member_id);
            wire::put_u32(&mut out, *session_timeout_ms);
            wire::put_u32(&mut out, topics.len() as u32);
            for t in topics {
                wire::put_str(&mut out, t);
            }
        }
        Request::Heartbeat {
            group,
            member_id,
            generation,
        } => {
            frame_header(&mut out, MSG_HEARTBEAT, correlation_id);
            wire::put_str(&mut out, group);
            wire::put_str(&mut out, member_id);
            wire::put_u64(&mut out, *generation);
        }
        Request::LeaveGroup { group, member_id } => {
            frame_header(&mut out, MSG_LEAVE_GROUP, correlation_id);
            wire::put_str(&mut out, group);
            wire::put_str(&mut out, member_id);
        }
    }
    finish_frame(out)
}

pub fn encode_response(resp: &Response, correlation_id: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    match resp {
        Response::CreateTopic { error } => {
            frame_header(&mut out, MSG_CREATE_TOPIC | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
        }
        Response::Metadata { error, cluster } => {
            frame_header(&mut out, MSG_METADATA | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            put_cluster_meta(&mut out, cluster);
        }
        Response::ReplicaFetch {
            error,
            leader_epoch,
            log_end,
            high_watermark,
            records,
        } => {
            frame_header(&mut out, MSG_REPLICA_FETCH | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_u64(&mut out, *leader_epoch);
            wire::put_u64(&mut out, *log_end);
            wire::put_u64(&mut out, *high_watermark);
            wire::put_u32(&mut out, records.len() as u32);
            for r in records {
                wire::put_u64(&mut out, r.offset);
                wire::put_i64(&mut out, r.timestamp_ms);
                wire::put_opt_bytes(&mut out, r.key.as_deref());
                wire::put_bytes(&mut out, &r.value);
            }
        }
        Response::EpochCheck { error, end_offset } => {
            frame_header(&mut out, MSG_EPOCH_CHECK | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_u64(&mut out, *end_offset);
        }
        Response::RegisterBroker {
            error,
            metadata_version,
        } => {
            frame_header(&mut out, MSG_REGISTER_BROKER | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_u64(&mut out, *metadata_version);
        }
        Response::BrokerHeartbeat {
            error,
            metadata_version,
        } => {
            frame_header(&mut out, MSG_BROKER_HEARTBEAT | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_u64(&mut out, *metadata_version);
        }
        Response::AlterIsr {
            error,
            metadata_version,
        } => {
            frame_header(&mut out, MSG_ALTER_ISR | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_u64(&mut out, *metadata_version);
        }
        Response::ControllerMetadata { error, cluster } => {
            frame_header(&mut out, MSG_CONTROLLER_METADATA | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            put_cluster_meta(&mut out, cluster);
        }
        Response::Produce {
            error,
            base_offset,
            count,
        } => {
            frame_header(&mut out, MSG_PRODUCE | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_u64(&mut out, *base_offset);
            wire::put_u32(&mut out, *count);
        }
        Response::Fetch {
            error,
            log_start,
            next_offset,
            records,
        } => {
            frame_header(&mut out, MSG_FETCH | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_u64(&mut out, *log_start);
            wire::put_u64(&mut out, *next_offset);
            wire::put_u32(&mut out, records.len() as u32);
            for r in records {
                wire::put_u64(&mut out, r.offset);
                wire::put_i64(&mut out, r.timestamp_ms);
                wire::put_opt_bytes(&mut out, r.key.as_deref());
                wire::put_bytes(&mut out, &r.value);
            }
        }
        Response::CommitOffset { error } => {
            frame_header(&mut out, MSG_COMMIT_OFFSET | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
        }
        Response::FetchOffset { error, offset } => {
            frame_header(&mut out, MSG_FETCH_OFFSET | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_i64(&mut out, *offset);
        }
        Response::JoinGroup {
            error,
            member_id,
            generation,
            assignment,
        } => {
            frame_header(&mut out, MSG_JOIN_GROUP | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_str(&mut out, member_id);
            wire::put_u64(&mut out, *generation);
            wire::put_u32(&mut out, assignment.len() as u32);
            for (topic, partition) in assignment {
                wire::put_str(&mut out, topic);
                wire::put_u32(&mut out, *partition);
            }
        }
        Response::Heartbeat { error } => {
            frame_header(&mut out, MSG_HEARTBEAT | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
        }
        Response::LeaveGroup { error } => {
            frame_header(&mut out, MSG_LEAVE_GROUP | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
        }
    }
    finish_frame(out)
}

fn read_header(r: &mut Reader<'_>) -> Result<(u8, u32)> {
    let version = r.u8()?;
    if version != VERSION {
        return Err(ProtoError::BadVersion(version));
    }
    let msg_type = r.u8()?;
    let correlation_id = r.u32()?;
    Ok((msg_type, correlation_id))
}

/// Decodes a request frame (the bytes after the length field).
pub fn decode_request(frame: &[u8]) -> Result<(u32, Request)> {
    let mut r = Reader::new(frame);
    let (msg_type, corr) = read_header(&mut r)?;
    let req = match msg_type {
        MSG_CREATE_TOPIC => Request::CreateTopic {
            topic: r.string()?,
            partitions: r.u32()?,
            replication_factor: r.u32()?,
        },
        MSG_METADATA => Request::Metadata,
        MSG_PRODUCE => {
            let topic = r.string()?;
            let partition = r.u32()?;
            let acks = Acks::from_u8(r.u8()?)?;
            let leader_epoch = r.u64()?;
            let count = r.u32()?;
            let mut records = Vec::new();
            for _ in 0..count {
                records.push(ProduceRecord {
                    key: r.opt_bytes()?,
                    value: r.bytes()?,
                });
            }
            Request::Produce {
                topic,
                partition,
                acks,
                leader_epoch,
                records,
            }
        }
        MSG_REPLICA_FETCH => Request::ReplicaFetch {
            topic: r.string()?,
            partition: r.u32()?,
            follower_id: r.u32()?,
            leader_epoch: r.u64()?,
            offset: r.u64()?,
            max_bytes: r.u32()?,
            max_wait_ms: r.u32()?,
        },
        MSG_EPOCH_CHECK => Request::EpochCheck {
            topic: r.string()?,
            partition: r.u32()?,
            epoch: r.u64()?,
        },
        MSG_REGISTER_BROKER => Request::RegisterBroker {
            broker_id: r.u32()?,
            addr: r.string()?,
            peer_addr: r.string()?,
        },
        MSG_BROKER_HEARTBEAT => Request::BrokerHeartbeat {
            broker_id: r.u32()?,
            metadata_version: r.u64()?,
        },
        MSG_ALTER_ISR => {
            let topic = r.string()?;
            let partition = r.u32()?;
            let leader_epoch = r.u64()?;
            let count = r.u32()?;
            let mut isr = Vec::new();
            for _ in 0..count {
                isr.push(r.u32()?);
            }
            Request::AlterIsr {
                topic,
                partition,
                leader_epoch,
                isr,
            }
        }
        MSG_CONTROLLER_METADATA => Request::ControllerMetadata,
        MSG_FETCH => Request::Fetch {
            topic: r.string()?,
            partition: r.u32()?,
            offset: r.u64()?,
            max_bytes: r.u32()?,
            max_wait_ms: r.u32()?,
        },
        MSG_COMMIT_OFFSET => Request::CommitOffset {
            group: r.string()?,
            topic: r.string()?,
            partition: r.u32()?,
            offset: r.u64()?,
            member_id: r.string()?,
            generation: r.u64()?,
        },
        MSG_FETCH_OFFSET => Request::FetchOffset {
            group: r.string()?,
            topic: r.string()?,
            partition: r.u32()?,
        },
        MSG_JOIN_GROUP => {
            let group = r.string()?;
            let member_id = r.string()?;
            let session_timeout_ms = r.u32()?;
            let count = r.u32()?;
            let mut topics = Vec::new();
            for _ in 0..count {
                topics.push(r.string()?);
            }
            Request::JoinGroup {
                group,
                member_id,
                session_timeout_ms,
                topics,
            }
        }
        MSG_HEARTBEAT => Request::Heartbeat {
            group: r.string()?,
            member_id: r.string()?,
            generation: r.u64()?,
        },
        MSG_LEAVE_GROUP => Request::LeaveGroup {
            group: r.string()?,
            member_id: r.string()?,
        },
        other => return Err(ProtoError::UnknownMsgType(other)),
    };
    r.expect_end()?;
    Ok((corr, req))
}

/// Decodes a response frame (the bytes after the length field).
pub fn decode_response(frame: &[u8]) -> Result<(u32, Response)> {
    let mut r = Reader::new(frame);
    let (msg_type, corr) = read_header(&mut r)?;
    let resp = match msg_type {
        t if t == MSG_CREATE_TOPIC | RESPONSE_BIT => Response::CreateTopic {
            error: ErrorCode::from_u16(r.u16()?)?,
        },
        t if t == MSG_METADATA | RESPONSE_BIT => Response::Metadata {
            error: ErrorCode::from_u16(r.u16()?)?,
            cluster: read_cluster_meta(&mut r)?,
        },
        t if t == MSG_REPLICA_FETCH | RESPONSE_BIT => {
            let error = ErrorCode::from_u16(r.u16()?)?;
            let leader_epoch = r.u64()?;
            let log_end = r.u64()?;
            let high_watermark = r.u64()?;
            let count = r.u32()?;
            let mut records = Vec::new();
            for _ in 0..count {
                records.push(FetchedRecord {
                    offset: r.u64()?,
                    timestamp_ms: r.i64()?,
                    key: r.opt_bytes()?,
                    value: r.bytes()?,
                });
            }
            Response::ReplicaFetch {
                error,
                leader_epoch,
                log_end,
                high_watermark,
                records,
            }
        }
        t if t == MSG_EPOCH_CHECK | RESPONSE_BIT => Response::EpochCheck {
            error: ErrorCode::from_u16(r.u16()?)?,
            end_offset: r.u64()?,
        },
        t if t == MSG_REGISTER_BROKER | RESPONSE_BIT => Response::RegisterBroker {
            error: ErrorCode::from_u16(r.u16()?)?,
            metadata_version: r.u64()?,
        },
        t if t == MSG_BROKER_HEARTBEAT | RESPONSE_BIT => Response::BrokerHeartbeat {
            error: ErrorCode::from_u16(r.u16()?)?,
            metadata_version: r.u64()?,
        },
        t if t == MSG_ALTER_ISR | RESPONSE_BIT => Response::AlterIsr {
            error: ErrorCode::from_u16(r.u16()?)?,
            metadata_version: r.u64()?,
        },
        t if t == MSG_CONTROLLER_METADATA | RESPONSE_BIT => Response::ControllerMetadata {
            error: ErrorCode::from_u16(r.u16()?)?,
            cluster: read_cluster_meta(&mut r)?,
        },
        t if t == MSG_PRODUCE | RESPONSE_BIT => Response::Produce {
            error: ErrorCode::from_u16(r.u16()?)?,
            base_offset: r.u64()?,
            count: r.u32()?,
        },
        t if t == MSG_FETCH | RESPONSE_BIT => {
            let error = ErrorCode::from_u16(r.u16()?)?;
            let log_start = r.u64()?;
            let next_offset = r.u64()?;
            let count = r.u32()?;
            let mut records = Vec::new();
            for _ in 0..count {
                records.push(FetchedRecord {
                    offset: r.u64()?,
                    timestamp_ms: r.i64()?,
                    key: r.opt_bytes()?,
                    value: r.bytes()?,
                });
            }
            Response::Fetch {
                error,
                log_start,
                next_offset,
                records,
            }
        }
        t if t == MSG_COMMIT_OFFSET | RESPONSE_BIT => Response::CommitOffset {
            error: ErrorCode::from_u16(r.u16()?)?,
        },
        t if t == MSG_FETCH_OFFSET | RESPONSE_BIT => Response::FetchOffset {
            error: ErrorCode::from_u16(r.u16()?)?,
            offset: r.i64()?,
        },
        t if t == MSG_JOIN_GROUP | RESPONSE_BIT => {
            let error = ErrorCode::from_u16(r.u16()?)?;
            let member_id = r.string()?;
            let generation = r.u64()?;
            let count = r.u32()?;
            let mut assignment = Vec::new();
            for _ in 0..count {
                assignment.push((r.string()?, r.u32()?));
            }
            Response::JoinGroup {
                error,
                member_id,
                generation,
                assignment,
            }
        }
        t if t == MSG_HEARTBEAT | RESPONSE_BIT => Response::Heartbeat {
            error: ErrorCode::from_u16(r.u16()?)?,
        },
        t if t == MSG_LEAVE_GROUP | RESPONSE_BIT => Response::LeaveGroup {
            error: ErrorCode::from_u16(r.u16()?)?,
        },
        other => return Err(ProtoError::UnknownMsgType(other)),
    };
    r.expect_end()?;
    Ok((corr, resp))
}

/// Reads one frame; `Ok(None)` on clean EOF at a frame boundary.
pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_buf);
    if len == 0 || len > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame length {len} out of bounds"),
        ));
    }
    let mut frame = vec![0u8; len as usize];
    reader.read_exact(&mut frame).await?;
    Ok(Some(frame))
}

pub async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
) -> std::io::Result<()> {
    writer.write_all(frame).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_request(req: Request) {
        let frame = encode_request(&req, 42);
        let (corr, decoded) = decode_request(&frame[4..]).expect("decode");
        assert_eq!(corr, 42);
        assert_eq!(decoded, req);
    }

    fn roundtrip_response(resp: Response) {
        let frame = encode_response(&resp, 7);
        let (corr, decoded) = decode_response(&frame[4..]).expect("decode");
        assert_eq!(corr, 7);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn request_roundtrips() {
        roundtrip_request(Request::CreateTopic {
            topic: "orders".into(),
            partitions: 8,
            replication_factor: 3,
        });
        roundtrip_request(Request::Metadata);
        roundtrip_request(Request::Produce {
            topic: "orders".into(),
            partition: 3,
            acks: Acks::All,
            leader_epoch: 7,
            records: vec![
                ProduceRecord {
                    key: None,
                    value: vec![],
                },
                ProduceRecord {
                    key: Some(b"k".to_vec()),
                    value: vec![0xAB; 100_000],
                },
            ],
        });
        roundtrip_request(Request::ReplicaFetch {
            topic: "t".into(),
            partition: 2,
            follower_id: 1,
            leader_epoch: 5,
            offset: 900,
            max_bytes: 1 << 20,
            max_wait_ms: 250,
        });
        roundtrip_request(Request::EpochCheck {
            topic: "t".into(),
            partition: 0,
            epoch: 4,
        });
        roundtrip_request(Request::RegisterBroker {
            broker_id: 2,
            addr: "127.0.0.1:9202".into(),
            peer_addr: "127.0.0.1:9202".into(),
        });
        roundtrip_request(Request::BrokerHeartbeat {
            broker_id: 2,
            metadata_version: 17,
        });
        roundtrip_request(Request::AlterIsr {
            topic: "t".into(),
            partition: 1,
            leader_epoch: 5,
            isr: vec![0, 2],
        });
        roundtrip_request(Request::ControllerMetadata);
        roundtrip_request(Request::Fetch {
            topic: "t".into(),
            partition: 0,
            offset: u64::MAX,
            max_bytes: 1 << 20,
            max_wait_ms: 500,
        });
        roundtrip_request(Request::CommitOffset {
            group: "g".into(),
            topic: "t".into(),
            partition: 1,
            offset: 99,
            member_id: "m-3".into(),
            generation: 7,
        });
        roundtrip_request(Request::FetchOffset {
            group: "g".into(),
            topic: "t".into(),
            partition: 1,
        });
        roundtrip_request(Request::JoinGroup {
            group: "g".into(),
            member_id: String::new(),
            session_timeout_ms: 3000,
            topics: vec!["a".into(), "b".into()],
        });
        roundtrip_request(Request::Heartbeat {
            group: "g".into(),
            member_id: "m-1".into(),
            generation: 4,
        });
        roundtrip_request(Request::LeaveGroup {
            group: "g".into(),
            member_id: "m-1".into(),
        });
    }

    #[test]
    fn response_roundtrips() {
        roundtrip_response(Response::CreateTopic {
            error: ErrorCode::TopicExists,
        });
        let cluster = ClusterMeta {
            version: 12,
            brokers: vec![(0, "127.0.0.1:9200".into()), (1, "127.0.0.1:9201".into())],
            peers: vec![(0, "broker-0:9000".into()), (1, "broker-1:9000".into())],
            topics: vec![TopicMeta {
                name: "orders".into(),
                partitions: vec![
                    PartitionMeta {
                        partition: 0,
                        leader: 1,
                        leader_epoch: 3,
                        replicas: vec![0, 1],
                        isr: vec![1],
                    },
                    PartitionMeta {
                        partition: 1,
                        leader: -1,
                        leader_epoch: 9,
                        replicas: vec![0, 1],
                        isr: vec![],
                    },
                ],
            }],
        };
        roundtrip_response(Response::Metadata {
            error: ErrorCode::None,
            cluster: cluster.clone(),
        });
        roundtrip_response(Response::ControllerMetadata {
            error: ErrorCode::None,
            cluster,
        });
        roundtrip_response(Response::ReplicaFetch {
            error: ErrorCode::None,
            leader_epoch: 4,
            log_end: 1000,
            high_watermark: 990,
            records: vec![FetchedRecord {
                offset: 998,
                timestamp_ms: 55,
                key: None,
                value: b"r".to_vec(),
            }],
        });
        roundtrip_response(Response::EpochCheck {
            error: ErrorCode::None,
            end_offset: 456,
        });
        roundtrip_response(Response::RegisterBroker {
            error: ErrorCode::None,
            metadata_version: 3,
        });
        roundtrip_response(Response::BrokerHeartbeat {
            error: ErrorCode::FencedEpoch,
            metadata_version: 4,
        });
        roundtrip_response(Response::AlterIsr {
            error: ErrorCode::NotLeader,
            metadata_version: 5,
        });
        roundtrip_response(Response::Produce {
            error: ErrorCode::None,
            base_offset: 12345,
            count: 10,
        });
        roundtrip_response(Response::Fetch {
            error: ErrorCode::None,
            log_start: 5,
            next_offset: 8,
            records: vec![FetchedRecord {
                offset: 7,
                timestamp_ms: -1,
                key: Some(vec![1, 2, 3]),
                value: b"value".to_vec(),
            }],
        });
        roundtrip_response(Response::CommitOffset {
            error: ErrorCode::None,
        });
        roundtrip_response(Response::FetchOffset {
            error: ErrorCode::None,
            offset: -1,
        });
        roundtrip_response(Response::JoinGroup {
            error: ErrorCode::None,
            member_id: "m-2".into(),
            generation: 9,
            assignment: vec![("t".into(), 0), ("t".into(), 3)],
        });
        roundtrip_response(Response::Heartbeat {
            error: ErrorCode::StaleGeneration,
        });
        roundtrip_response(Response::LeaveGroup {
            error: ErrorCode::UnknownMember,
        });
    }

    #[test]
    fn truncated_frames_rejected() {
        let req = Request::Produce {
            topic: "t".into(),
            partition: 0,
            acks: Acks::Written,
            leader_epoch: 0,
            records: vec![ProduceRecord {
                key: Some(b"key".to_vec()),
                value: b"value".to_vec(),
            }],
        };
        let frame = encode_request(&req, 1);
        let body = &frame[4..];
        for cut in 0..body.len() {
            assert!(
                decode_request(&body[..cut]).is_err(),
                "truncation at {cut} bytes must not decode"
            );
        }
    }

    #[test]
    fn garbage_headers_rejected() {
        assert!(matches!(
            decode_request(&[9, 1, 0, 0, 0, 0]),
            Err(ProtoError::BadVersion(9))
        ));
        assert!(matches!(
            decode_request(&[VERSION, 0x7F, 0, 0, 0, 0]),
            Err(ProtoError::UnknownMsgType(0x7F))
        ));
        let mut frame = encode_request(&Request::Metadata, 3);
        frame.push(0xEE);
        assert!(decode_request(&frame[4..]).is_err(), "trailing bytes must fail");
    }
}
