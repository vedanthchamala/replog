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
}

impl Acks {
    fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => Self::None,
            1 => Self::Written,
            2 => Self::Durable,
            _ => return Err(ProtoError::Malformed(format!("unknown acks value {v}"))),
        })
    }
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
    },
    Metadata,
    Produce {
        topic: String,
        partition: u32,
        acks: Acks,
        records: Vec<ProduceRecord>,
    },
    Fetch {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    },
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
        topics: Vec<(String, u32)>,
    },
    Produce {
        error: ErrorCode,
        base_offset: u64,
        count: u32,
    },
    Fetch {
        error: ErrorCode,
        log_start: u64,
        next_offset: u64,
        records: Vec<FetchedRecord>,
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
        Request::CreateTopic { topic, partitions } => {
            frame_header(&mut out, MSG_CREATE_TOPIC, correlation_id);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partitions);
        }
        Request::Metadata => {
            frame_header(&mut out, MSG_METADATA, correlation_id);
        }
        Request::Produce {
            topic,
            partition,
            acks,
            records,
        } => {
            frame_header(&mut out, MSG_PRODUCE, correlation_id);
            wire::put_str(&mut out, topic);
            wire::put_u32(&mut out, *partition);
            wire::put_u8(&mut out, *acks as u8);
            wire::put_u32(&mut out, records.len() as u32);
            for r in records {
                wire::put_opt_bytes(&mut out, r.key.as_deref());
                wire::put_bytes(&mut out, &r.value);
            }
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
        Response::Metadata { error, topics } => {
            frame_header(&mut out, MSG_METADATA | RESPONSE_BIT, correlation_id);
            wire::put_u16(&mut out, *error as u16);
            wire::put_u32(&mut out, topics.len() as u32);
            for (name, partitions) in topics {
                wire::put_str(&mut out, name);
                wire::put_u32(&mut out, *partitions);
            }
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
        },
        MSG_METADATA => Request::Metadata,
        MSG_PRODUCE => {
            let topic = r.string()?;
            let partition = r.u32()?;
            let acks = Acks::from_u8(r.u8()?)?;
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
                records,
            }
        }
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
        t if t == MSG_METADATA | RESPONSE_BIT => {
            let error = ErrorCode::from_u16(r.u16()?)?;
            let count = r.u32()?;
            let mut topics = Vec::new();
            for _ in 0..count {
                topics.push((r.string()?, r.u32()?));
            }
            Response::Metadata { error, topics }
        }
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
        });
        roundtrip_request(Request::Metadata);
        roundtrip_request(Request::Produce {
            topic: "orders".into(),
            partition: 3,
            acks: Acks::Durable,
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
        roundtrip_response(Response::Metadata {
            error: ErrorCode::None,
            topics: vec![("a".into(), 1), ("b".into(), 16)],
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
