//! Cluster mode: the broker's replication machinery.
//!
//! One `Replica` per hosted partition. Leaders track follower progress from
//! ReplicaFetch offsets, maintain the ISR (asking the controller to shrink or
//! expand it — the controller owns the tuple, the leader proposes), advance
//! the high-water mark as min(ISR match offsets, own log end), and park
//! acks=all produces until the HWM covers them. Followers run a fetcher task:
//! reconcile history via EpochCheck (truncate the divergent suffix), then pull
//! and append verbatim.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, watch};

use crate::broker::partition::PartitionHandle;
use crate::client::{ClientError, Connection};
use crate::proto::{ErrorCode, Request, Response};

pub struct Replica {
    pub topic: String,
    pub partition: u32,
    pub handle: PartitionHandle,
    pub state: Mutex<ReplState>,
    pub hwm_tx: watch::Sender<u64>,
    pub hwm_rx: watch::Receiver<u64>,
}

pub struct ReplState {
    pub epoch: u64,
    pub is_leader: bool,
    pub isr: Vec<u32>,
    pub replicas: Vec<u32>,
    /// follower id -> (next offset it asked for, when it was last *caught
    /// up* — i.e. fetched at or past the leader's log end). ISR membership
    /// keys off the second field, Kafka's lastCaughtUpTimeMs move: "was
    /// caught up recently", not "was caught up once" (a dead follower's
    /// match offset stays maxed forever) and not "fetched recently" (a
    /// perpetually lagging follower fetches constantly without catching up).
    pub match_offsets: HashMap<u32, (u64, Instant)>,
    /// acks=all produces waiting for the HWM: (last_offset, base, count, reply)
    pub pending_all: Vec<(u64, u64, u32, oneshot::Sender<Result<(u64, u32), ErrorCode>>)>,
    /// Bumped on every role/epoch change; running fetchers exit when stale.
    pub fetcher_generation: u64,
    pub fetcher_alive: bool,
    /// When this broker became leader — the lag clock for followers that
    /// have not fetched yet.
    pub leader_since: Instant,
}

impl Replica {
    pub fn new(topic: String, partition: u32, handle: PartitionHandle) -> Arc<Self> {
        let (hwm_tx, hwm_rx) = watch::channel(0u64);
        Arc::new(Self {
            topic,
            partition,
            handle,
            state: Mutex::new(ReplState {
                epoch: 0,
                is_leader: false,
                isr: Vec::new(),
                replicas: Vec::new(),
                match_offsets: HashMap::new(),
                pending_all: Vec::new(),
                fetcher_generation: 0,
                fetcher_alive: false,
                leader_since: Instant::now(),
            }),
            hwm_tx,
            hwm_rx,
        })
    }

    /// Recomputes the HWM (advance-only) and completes covered acks=all
    /// produces. Called after leader appends, follower fetch progress, and
    /// ISR changes.
    pub fn advance_hwm(&self, self_id: u32) {
        let log_end = *self.handle.next_offset.borrow();
        let ready = {
            let mut state = self.state.lock().unwrap();
            if !state.is_leader {
                return;
            }
            let mut hwm = log_end;
            for follower in state.isr.iter().filter(|id| **id != self_id) {
                let matched = state
                    .match_offsets
                    .get(follower)
                    .map(|&(offset, _)| offset)
                    .unwrap_or(0);
                hwm = hwm.min(matched);
            }
            let current = *self.hwm_rx.borrow();
            if hwm > current {
                self.hwm_tx.send_replace(hwm);
            }
            drain_pending(&mut state, hwm.max(current))
        };
        for (base, count, reply) in ready {
            let _ = reply.send(Ok((base, count)));
        }
    }
}

/// Drains `pending_all` entries covered by `hwm`, returning their replies.
pub fn drain_pending(
    state: &mut ReplState,
    hwm: u64,
) -> Vec<(u64, u32, oneshot::Sender<Result<(u64, u32), ErrorCode>>)> {
    let mut ready = Vec::new();
    let mut keep = Vec::new();
    for entry in state.pending_all.drain(..) {
        if entry.0 < hwm {
            ready.push((entry.1, entry.2, entry.3));
        } else {
            keep.push(entry);
        }
    }
    state.pending_all = keep;
    ready
}

/// The follower's pull loop for one partition under one leader epoch. On any
/// exit it marks itself dead (and pings the runtime), so a replacement can be
/// spawned even when cluster metadata did not change.
pub async fn run_fetcher(
    replica: Arc<Replica>,
    self_id: u32,
    epoch: u64,
    generation: u64,
    leader_addr: String,
    refresh: tokio::sync::mpsc::UnboundedSender<()>,
) {
    fetcher_loop(&replica, self_id, epoch, generation, leader_addr, &refresh).await;
    let mut state = replica.state.lock().unwrap();
    if state.fetcher_generation == generation {
        state.fetcher_alive = false;
    }
}

async fn fetcher_loop(
    replica: &Arc<Replica>,
    self_id: u32,
    epoch: u64,
    generation: u64,
    leader_addr: String,
    refresh: &tokio::sync::mpsc::UnboundedSender<()>,
) {
    let is_current = || {
        let state = replica.state.lock().unwrap();
        !state.is_leader && state.fetcher_generation == generation
    };

    let conn = loop {
        if !is_current() {
            return;
        }
        match Connection::connect(&leader_addr).await {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };

    // Reconcile: where did my last epoch end on the leader? Truncate the
    // divergent suffix before fetching (KIP-101's move).
    let (mut local_epoch, _) = match replica.handle.epoch_status().await {
        Ok(s) => s,
        Err(_) => return,
    };
    match conn
        .call(&Request::EpochCheck {
            topic: replica.topic.clone(),
            partition: replica.partition,
            epoch: local_epoch,
        })
        .await
    {
        Ok(Response::EpochCheck {
            error: ErrorCode::None,
            end_offset,
        }) => {
            // Re-check after the await: a role/epoch change (or an aborted
            // broker in tests) while the reply was in flight means this
            // fetcher no longer speaks for the replica — truncating now
            // would mutate a log it doesn't own anymore.
            if !is_current() {
                return;
            }
            let local_end = *replica.handle.next_offset.borrow();
            let target = end_offset.min(local_end);
            if target < local_end {
                eprintln!(
                    "replica {}-{}: truncating divergent suffix {} -> {} (leader said epoch {} ended at {})",
                    replica.topic, replica.partition, local_end, target, local_epoch, end_offset
                );
                if replica.handle.truncate_to(target).await.is_err() {
                    return;
                }
            }
        }
        Ok(Response::EpochCheck { .. }) | Ok(_) => {
            let _ = refresh.send(());
            return;
        }
        Err(_) => {
            let _ = refresh.send(());
            return;
        }
    }

    loop {
        if !is_current() {
            return;
        }
        let offset = *replica.handle.next_offset.borrow();
        let resp = conn
            .call(&Request::ReplicaFetch {
                topic: replica.topic.clone(),
                partition: replica.partition,
                follower_id: self_id,
                leader_epoch: epoch,
                offset,
                max_bytes: 1 << 20,
                max_wait_ms: 250,
            })
            .await;
        match resp {
            Ok(Response::ReplicaFetch {
                error: ErrorCode::None,
                leader_epoch,
                records,
                ..
            }) => {
                // Same re-check as reconciliation: the response was in
                // flight while our claim on this replica may have lapsed;
                // appending stale records into a new epoch's log is exactly
                // the divergence this machinery exists to prevent.
                if !is_current() {
                    return;
                }
                if leader_epoch > local_epoch {
                    replica.handle.record_epoch(leader_epoch);
                    local_epoch = leader_epoch;
                }
                if !records.is_empty() {
                    let converted = records
                        .into_iter()
                        .map(|r| crate::storage::Record {
                            offset: r.offset,
                            timestamp_ms: r.timestamp_ms,
                            key: r.key,
                            value: r.value,
                        })
                        .collect();
                    if replica.handle.append_replicated(converted).await.is_err() {
                        return;
                    }
                }
            }
            Ok(Response::ReplicaFetch { .. }) => {
                // NotLeader / FencedEpoch / anything else: our view is stale.
                let _ = refresh.send(());
                return;
            }
            Ok(_) => return,
            Err(ClientError::Closed) | Err(ClientError::Io(_)) => {
                let _ = refresh.send(());
                return;
            }
            Err(_) => return,
        }
    }
}
