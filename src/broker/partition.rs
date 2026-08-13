//! One writer thread per partition, owning its `Log` (single-writer principle).
//!
//! The thread is a plain OS thread, not a tokio task: appends and fsyncs are
//! blocking disk work (an F_FULLFSYNC is ~4 ms on this hardware) and must not
//! stall an async worker. Commands arrive on an mpsc channel; replies travel
//! on tokio oneshots, which may be completed from any thread.
//!
//! The ack contract lives here: an `Acks::Durable` produce is parked in a
//! pending queue until the covering flush advances `durable_offset` past it.
//! The thread's recv timeout doubles as the group-commit timer, so a lone
//! record's ack is never stranded waiting for a next append to trip the flush.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, watch};

use crate::proto::{Acks, ErrorCode};
use crate::storage::{FsyncPolicy, Log, LogConfig, Record, StorageError};

pub struct ReadOk {
    pub log_start: u64,
    pub next_offset: u64,
    pub records: Vec<Record>,
}

type AppendResult = Result<(u64, u32), ErrorCode>;

pub enum Cmd {
    Append {
        records: Vec<(Option<Vec<u8>>, Vec<u8>)>,
        acks: Acks,
        reply: Option<oneshot::Sender<AppendResult>>,
    },
    Read {
        offset: u64,
        max_bytes: u64,
        reply: oneshot::Sender<Result<ReadOk, ErrorCode>>,
    },
}

#[derive(Clone)]
pub struct PartitionHandle {
    tx: Sender<Cmd>,
    /// Published by the writer thread after every append; long-poll fetches
    /// wait on it instead of busy-polling.
    pub next_offset: watch::Receiver<u64>,
}

impl PartitionHandle {
    /// Enqueues an append immediately (preserving arrival order from the
    /// caller) and returns the receiver to await the ack on.
    pub fn append_start(
        &self,
        records: Vec<(Option<Vec<u8>>, Vec<u8>)>,
        acks: Acks,
    ) -> oneshot::Receiver<AppendResult> {
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(Cmd::Append {
                records,
                acks,
                reply: Some(tx),
            })
            .is_err()
        {
            // Receiver sees the dropped sender as a closed channel.
        }
        rx
    }

    pub fn append_no_reply(&self, records: Vec<(Option<Vec<u8>>, Vec<u8>)>) {
        let _ = self.tx.send(Cmd::Append {
            records,
            acks: Acks::None,
            reply: None,
        });
    }

    pub async fn append(
        &self,
        records: Vec<(Option<Vec<u8>>, Vec<u8>)>,
        acks: Acks,
    ) -> AppendResult {
        self.append_start(records, acks)
            .await
            .unwrap_or(Err(ErrorCode::Storage))
    }

    pub async fn read(&self, offset: u64, max_bytes: u64) -> Result<ReadOk, ErrorCode> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Read {
                offset,
                max_bytes,
                reply: tx,
            })
            .map_err(|_| ErrorCode::Storage)?;
        rx.await.unwrap_or(Err(ErrorCode::Storage))
    }
}

pub fn spawn(
    dir: &Path,
    config: LogConfig,
) -> crate::storage::Result<(PartitionHandle, std::thread::JoinHandle<()>)> {
    let log = Log::open(dir, config.clone())?;
    let (tx, rx) = mpsc::channel();
    let (watch_tx, watch_rx) = watch::channel(log.next_offset());
    let name = dir.file_name().map_or_else(String::new, |n| n.to_string_lossy().into_owned());
    let join = std::thread::Builder::new()
        .name(format!("partition-{name}"))
        .spawn(move || run(log, rx, watch_tx, config.fsync))
        .expect("spawn partition thread");
    Ok((
        PartitionHandle {
            tx,
            next_offset: watch_rx,
        },
        join,
    ))
}

struct Pending {
    last_offset: u64,
    base_offset: u64,
    count: u32,
    reply: oneshot::Sender<AppendResult>,
}

fn is_dirty(log: &Log) -> bool {
    let next = log.next_offset();
    next > 0 && log.durable_offset() != Some(next - 1)
}

fn map_storage_err(e: &StorageError) -> ErrorCode {
    match e {
        StorageError::OffsetOutOfRange(_) => ErrorCode::OffsetOutOfRange,
        StorageError::RecordTooLarge(_) => ErrorCode::Malformed,
        _ => ErrorCode::Storage,
    }
}

fn run(mut log: Log, rx: Receiver<Cmd>, watch_tx: watch::Sender<u64>, policy: FsyncPolicy) {
    let batch_max_ms = match policy {
        FsyncPolicy::Batch { max_ms, .. } => Some(max_ms),
        _ => None,
    };
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut dirty_since: Option<Instant> = None;

    loop {
        let timeout = match (dirty_since, batch_max_ms) {
            (Some(since), Some(ms)) => Duration::from_millis(ms)
                .saturating_sub(since.elapsed())
                .max(Duration::from_millis(1)),
            _ => Duration::from_millis(500),
        };
        match rx.recv_timeout(timeout) {
            Ok(Cmd::Append {
                records,
                acks,
                reply,
            }) => {
                let mut base_offset = None;
                let mut count = 0u32;
                let mut error = None;
                for (key, value) in records {
                    match log.append(key, value) {
                        Ok(info) => {
                            base_offset.get_or_insert(info.offset);
                            count += 1;
                        }
                        Err(e) => {
                            eprintln!("partition append failed: {e}");
                            error = Some(map_storage_err(&e));
                            break;
                        }
                    }
                }
                watch_tx.send_replace(log.next_offset());
                dirty_since = if is_dirty(&log) {
                    dirty_since.or_else(|| Some(Instant::now()))
                } else {
                    None
                };
                if let Some(reply) = reply {
                    match (error, base_offset) {
                        (Some(code), _) => {
                            let _ = reply.send(Err(code));
                        }
                        (None, None) => {
                            let _ = reply.send(Ok((log.next_offset(), 0)));
                        }
                        (None, Some(base)) => {
                            let last = log.next_offset() - 1;
                            let acked_now = match acks {
                                Acks::None | Acks::Written => true,
                                // A broker that never flushes cannot promise
                                // more than "written"; don't strand the ack.
                                Acks::Durable => matches!(policy, FsyncPolicy::Os)
                                    || log.durable_offset().is_some_and(|d| d >= last),
                            };
                            if acked_now {
                                let _ = reply.send(Ok((base, count)));
                            } else {
                                pending.push_back(Pending {
                                    last_offset: last,
                                    base_offset: base,
                                    count,
                                    reply,
                                });
                            }
                        }
                    }
                }
                drain_pending(&log, &mut pending);
            }
            Ok(Cmd::Read {
                offset,
                max_bytes,
                reply,
            }) => {
                let result = log
                    .read(offset, max_bytes)
                    .map(|records| ReadOk {
                        log_start: log.start_offset(),
                        next_offset: log.next_offset(),
                        records,
                    })
                    .map_err(|e| {
                        if !matches!(e, StorageError::OffsetOutOfRange(_)) {
                            eprintln!("partition read failed: {e}");
                        }
                        map_storage_err(&e)
                    });
                let _ = reply.send(result);
            }
            Err(RecvTimeoutError::Timeout) => {
                if let (Some(since), Some(ms)) = (dirty_since, batch_max_ms)
                    && since.elapsed().as_millis() as u64 >= ms
                {
                    flush(&mut log, &mut dirty_since, &mut pending);
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                flush(&mut log, &mut dirty_since, &mut pending);
                break;
            }
        }
    }
}

fn flush(log: &mut Log, dirty_since: &mut Option<Instant>, pending: &mut VecDeque<Pending>) {
    if let Err(e) = log.flush() {
        eprintln!("partition flush failed: {e}");
        return;
    }
    *dirty_since = None;
    drain_pending(log, pending);
}

fn drain_pending(log: &Log, pending: &mut VecDeque<Pending>) {
    let durable = log.durable_offset();
    while let Some(front) = pending.front() {
        if durable.is_some_and(|d| d >= front.last_offset) {
            let p = pending.pop_front().unwrap();
            let _ = p.reply.send(Ok((p.base_offset, p.count)));
        } else {
            break;
        }
    }
}
