//! Client side: a pipelined connection plus thin Producer/Consumer wrappers.
//!
//! `Connection` routes responses back to callers by correlation ID, so any
//! number of requests can be in flight on one socket — the broker may answer
//! them out of order (long-poll fetches) and each still lands with its caller.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

use crate::proto::{
    self, Acks, ErrorCode, FetchedRecord, ProduceRecord, Request, Response,
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection closed")]
    Closed,
    #[error("protocol: {0}")]
    Proto(#[from] proto::ProtoError),
    #[error("broker error: {0:?}")]
    Broker(ErrorCode),
    #[error("unexpected response type")]
    UnexpectedResponse,
}

pub type Result<T> = std::result::Result<T, ClientError>;

struct Inner {
    tx: UnboundedSender<Vec<u8>>,
    pending: Mutex<HashMap<u32, oneshot::Sender<Response>>>,
    next_corr: AtomicU32,
}

#[derive(Clone)]
pub struct Connection {
    inner: Arc<Inner>,
}

impl Connection {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (mut rd, mut wr) = stream.into_split();
        let (tx, mut out_rx) = unbounded_channel::<Vec<u8>>();
        let inner = Arc::new(Inner {
            tx,
            pending: Mutex::new(HashMap::new()),
            next_corr: AtomicU32::new(1),
        });

        tokio::spawn(async move {
            while let Some(frame) = out_rx.recv().await {
                if wr.write_all(&frame).await.is_err() {
                    break;
                }
            }
            let _ = wr.shutdown().await;
        });

        let weak: Weak<Inner> = Arc::downgrade(&inner);
        tokio::spawn(async move {
            loop {
                match proto::read_frame(&mut rd).await {
                    Ok(Some(frame)) => match proto::decode_response(&frame) {
                        Ok((corr, resp)) => {
                            let Some(inner) = weak.upgrade() else { break };
                            if let Some(waiter) = inner.pending.lock().unwrap().remove(&corr) {
                                let _ = waiter.send(resp);
                            }
                        }
                        Err(e) => {
                            eprintln!("client: undecodable response frame: {e}");
                            break;
                        }
                    },
                    _ => break,
                }
            }
            // Dropping the parked senders turns every in-flight call into
            // ClientError::Closed.
            if let Some(inner) = weak.upgrade() {
                inner.pending.lock().unwrap().clear();
            }
        });

        Ok(Self { inner })
    }

    /// Sends a request and returns the receiver its response will land on;
    /// lets callers pipeline before awaiting.
    pub fn call_start(&self, req: &Request) -> Result<oneshot::Receiver<Response>> {
        let corr = self.inner.next_corr.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().unwrap().insert(corr, tx);
        let frame = proto::encode_request(req, corr);
        self.inner.tx.send(frame).map_err(|_| ClientError::Closed)?;
        Ok(rx)
    }

    pub async fn call(&self, req: &Request) -> Result<Response> {
        self.call_start(req)?.await.map_err(|_| ClientError::Closed)
    }

    /// Fire-and-forget send with no response expected (acks=0 produces).
    pub fn send_only(&self, req: &Request) -> Result<()> {
        let corr = self.inner.next_corr.fetch_add(1, Ordering::Relaxed);
        let frame = proto::encode_request(req, corr);
        self.inner.tx.send(frame).map_err(|_| ClientError::Closed)
    }

    pub async fn create_topic(&self, topic: &str, partitions: u32) -> Result<()> {
        match self
            .call(&Request::CreateTopic {
                topic: topic.to_string(),
                partitions,
            })
            .await?
        {
            Response::CreateTopic {
                error: ErrorCode::None,
            } => Ok(()),
            Response::CreateTopic { error } => Err(ClientError::Broker(error)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn metadata(&self) -> Result<Vec<(String, u32)>> {
        match self.call(&Request::Metadata).await? {
            Response::Metadata {
                error: ErrorCode::None,
                topics,
            } => Ok(topics),
            Response::Metadata { error, .. } => Err(ClientError::Broker(error)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Returns the batch's base offset, or `None` for acks=0 (no response).
    pub async fn produce(
        &self,
        topic: &str,
        partition: u32,
        acks: Acks,
        records: Vec<ProduceRecord>,
    ) -> Result<Option<u64>> {
        let req = Request::Produce {
            topic: topic.to_string(),
            partition,
            acks,
            records,
        };
        if acks == Acks::None {
            self.send_only(&req)?;
            return Ok(None);
        }
        match self.call(&req).await? {
            Response::Produce {
                error: ErrorCode::None,
                base_offset,
                ..
            } => Ok(Some(base_offset)),
            Response::Produce { error, .. } => Err(ClientError::Broker(error)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Returns (log_start, next_offset, records).
    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    ) -> Result<(u64, u64, Vec<FetchedRecord>)> {
        match self
            .call(&Request::Fetch {
                topic: topic.to_string(),
                partition,
                offset,
                max_bytes,
                max_wait_ms,
            })
            .await?
        {
            Response::Fetch {
                error: ErrorCode::None,
                log_start,
                next_offset,
                records,
            } => Ok((log_start, next_offset, records)),
            Response::Fetch { error, .. } => Err(ClientError::Broker(error)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn commit_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        match self
            .call(&Request::CommitOffset {
                group: group.to_string(),
                topic: topic.to_string(),
                partition,
                offset,
            })
            .await?
        {
            Response::CommitOffset {
                error: ErrorCode::None,
            } => Ok(()),
            Response::CommitOffset { error } => Err(ClientError::Broker(error)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn fetch_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>> {
        match self
            .call(&Request::FetchOffset {
                group: group.to_string(),
                topic: topic.to_string(),
                partition,
            })
            .await?
        {
            Response::FetchOffset {
                error: ErrorCode::None,
                offset,
            } => Ok((offset >= 0).then_some(offset as u64)),
            Response::FetchOffset { error, .. } => Err(ClientError::Broker(error)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }
}

/// Buffers records and ships them as one Produce per `batch_records`.
/// Client-side batching is the Stage 2 bench's independent variable, so it is
/// explicit here rather than hidden behind a linger timer.
pub struct Producer {
    conn: Connection,
    topic: String,
    partition: u32,
    acks: Acks,
    batch_records: usize,
    buf: Vec<ProduceRecord>,
}

impl Producer {
    pub fn new(
        conn: Connection,
        topic: impl Into<String>,
        partition: u32,
        acks: Acks,
        batch_records: usize,
    ) -> Self {
        Self {
            conn,
            topic: topic.into(),
            partition,
            acks,
            batch_records: batch_records.max(1),
            buf: Vec::new(),
        }
    }

    /// Buffers one record; flushes when the batch is full. Returns the flushed
    /// batch's base offset when a flush happened.
    pub async fn send(&mut self, key: Option<Vec<u8>>, value: Vec<u8>) -> Result<Option<u64>> {
        self.buf.push(ProduceRecord { key, value });
        if self.buf.len() >= self.batch_records {
            self.flush().await
        } else {
            Ok(None)
        }
    }

    pub async fn flush(&mut self) -> Result<Option<u64>> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let records = std::mem::take(&mut self.buf);
        self.conn
            .produce(&self.topic, self.partition, self.acks, records)
            .await
    }
}

/// A single-partition consumer tracking its own position, with optional
/// group-named commit/resume. (Group *membership* — coordinated assignment,
/// rebalancing — is Stage 3; here a group is only a name to store offsets by.)
pub struct Consumer {
    conn: Connection,
    topic: String,
    partition: u32,
    group: Option<String>,
    position: u64,
    pub max_bytes: u32,
    pub max_wait_ms: u32,
}

impl Consumer {
    pub fn start_at(
        conn: Connection,
        topic: impl Into<String>,
        partition: u32,
        position: u64,
    ) -> Self {
        Self {
            conn,
            topic: topic.into(),
            partition,
            group: None,
            position,
            max_bytes: 1 << 20,
            max_wait_ms: 0,
        }
    }

    /// Starts from the group's committed offset (or 0 if none).
    pub async fn resume(
        conn: Connection,
        topic: impl Into<String>,
        partition: u32,
        group: impl Into<String>,
    ) -> Result<Self> {
        let topic = topic.into();
        let group = group.into();
        let committed = conn.fetch_offset(&group, &topic, partition).await?;
        Ok(Self {
            conn,
            topic,
            partition,
            group: Some(group),
            position: committed.unwrap_or(0),
            max_bytes: 1 << 20,
            max_wait_ms: 0,
        })
    }

    pub async fn poll(&mut self) -> Result<Vec<FetchedRecord>> {
        let (_, _, records) = self
            .conn
            .fetch(
                &self.topic,
                self.partition,
                self.position,
                self.max_bytes,
                self.max_wait_ms,
            )
            .await?;
        if let Some(last) = records.last() {
            self.position = last.offset + 1;
        }
        Ok(records)
    }

    /// Commits the current position (the next offset to read) under the group.
    pub async fn commit(&self) -> Result<()> {
        let group = self
            .group
            .as_deref()
            .ok_or(ClientError::Broker(ErrorCode::Malformed))?;
        self.conn
            .commit_offset(group, &self.topic, self.partition, self.position)
            .await
    }

    pub fn position(&self) -> u64 {
        self.position
    }
}
