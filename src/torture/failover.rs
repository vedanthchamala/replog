//! Failover decomposition and the per-second ack timeline.
//!
//! A failover gap is what a client lives through between a fault and its next
//! successful ack. Splitting it needs two clocks that the harness owns, not
//! the target: a **leader watch** that polls the metadata API every 20 ms and
//! records every leader change it sees (t1 = first time a partition's leader
//! is someone other than the victim), and an **ack clock** the producers stamp
//! on every successful produce (t2 = first ack after the fault). Both are
//! client-side observations, so `t1 − t0` (detection + election, as visible)
//! and `t2 − t1` (client recovery: refresh, reconnect, retry backoff) mean the
//! same thing for every target.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::cluster::{FaultTarget, majority, views};

#[derive(Clone, Debug)]
pub struct LeaderChange {
    pub at: Instant,
    pub partition: u32,
    pub old: i32,
    pub new: i32,
}

#[derive(Default)]
struct LeaderState {
    current: Vec<i32>,
    changes: Vec<LeaderChange>,
    /// Most recent per-broker answers (None = did not answer).
    latest_views: Vec<Option<Vec<i32>>>,
}

#[derive(Clone)]
pub struct LeaderWatch {
    state: Arc<Mutex<LeaderState>>,
}

impl LeaderWatch {
    pub fn spawn<T: FaultTarget + 'static>(
        target: Arc<T>,
        topic: String,
        partitions: u32,
        interval: Duration,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> LeaderWatch {
        let state = Arc::new(Mutex::new(LeaderState {
            current: vec![-1; partitions as usize],
            changes: Vec::new(),
            latest_views: Vec::new(),
        }));
        let watch = LeaderWatch { state: state.clone() };
        tokio::spawn(async move {
            loop {
                if *stop.borrow() {
                    return;
                }
                let per_broker = views(target.as_ref(), &topic, partitions).await;
                let view = majority(&per_broker, partitions);
                let now = Instant::now();
                {
                    let mut st = state.lock().unwrap();
                    st.latest_views = per_broker;
                    for (p, &l) in view.iter().enumerate() {
                        let old = st.current[p];
                        if l != old {
                            st.changes.push(LeaderChange {
                                at: now,
                                partition: p as u32,
                                old,
                                new: l,
                            });
                            st.current[p] = l;
                        }
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = stop.changed() => return,
                }
            }
        });
        watch
    }

    pub fn current(&self) -> Vec<i32> {
        self.state.lock().unwrap().current.clone()
    }

    /// Latest per-broker opinions, for spotting a zombie that still claims
    /// leadership after the majority moved on.
    pub fn latest_views(&self) -> Vec<Option<Vec<i32>>> {
        self.state.lock().unwrap().latest_views.clone()
    }

    /// First observation after `t0` of a live leader for `partition` that is
    /// not `victim`.
    pub fn first_new_leader_after(&self, t0: Instant, partition: u32, victim: usize) -> Option<LeaderChange> {
        self.state
            .lock()
            .unwrap()
            .changes
            .iter()
            .find(|c| c.at >= t0 && c.partition == partition && c.new >= 0 && c.new as usize != victim)
            .cloned()
    }

    pub fn changes(&self) -> Vec<LeaderChange> {
        self.state.lock().unwrap().changes.clone()
    }
}

/// Per-partition "last successful ack" stamp and running count. Producers
/// call `record`; the schedule polls `last_ack` to find t2 after a fault.
pub struct AckClock {
    epoch: Instant,
    last_ack_ns: Vec<AtomicU64>,
    acked: Vec<AtomicU64>,
    /// Largest interval between consecutive acks since the last `reset_gap`.
    gap_max_ns: Vec<AtomicU64>,
}

impl AckClock {
    pub fn new(partitions: u32, epoch: Instant) -> AckClock {
        AckClock {
            epoch,
            last_ack_ns: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            acked: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            gap_max_ns: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    pub fn record(&self, partition: u32, n: usize) {
        let p = partition as usize;
        let ns = (self.epoch.elapsed().as_nanos() as u64).max(1);
        // 0 means "never"; a real stamp is always > 0 after epoch.
        let prev = self.last_ack_ns[p].swap(ns, Ordering::Relaxed);
        if prev > 0 {
            let gap = ns.saturating_sub(prev);
            self.gap_max_ns[p].fetch_max(gap, Ordering::Relaxed);
        }
        self.acked[p].fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Start a fresh gap measurement (call right before a fault).
    pub fn reset_gap(&self, partition: u32) {
        self.gap_max_ns[partition as usize].store(0, Ordering::Relaxed);
    }

    /// Largest ack-to-ack interval since the reset, including a gap that is
    /// still open right now (no ack since `last_ack`). This is the client's
    /// unavailability window, immune to a straggling in-flight ack landing
    /// just after the fault.
    pub fn max_gap(&self, partition: u32) -> Option<Duration> {
        let p = partition as usize;
        let recorded = self.gap_max_ns[p].load(Ordering::Relaxed);
        let open = match self.last_ack_ns[p].load(Ordering::Relaxed) {
            0 => 0,
            last => (self.epoch.elapsed().as_nanos() as u64).saturating_sub(last),
        };
        let g = recorded.max(open);
        (g > 0).then(|| Duration::from_nanos(g))
    }

    pub fn last_ack(&self, partition: u32) -> Option<Instant> {
        match self.last_ack_ns[partition as usize].load(Ordering::Relaxed) {
            0 => None,
            ns => Some(self.epoch + Duration::from_nanos(ns)),
        }
    }

    pub fn acked(&self, partition: u32) -> u64 {
        self.acked[partition as usize].load(Ordering::Relaxed)
    }

    pub fn partitions(&self) -> u32 {
        self.acked.len() as u32
    }

    /// Wait until some ack on `partition` is stamped after `t0`, or `deadline`.
    pub async fn first_ack_after(&self, partition: u32, t0: Instant, deadline: Instant) -> Option<Instant> {
        loop {
            if let Some(t) = self.last_ack(partition) {
                if t > t0 {
                    return Some(t);
                }
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// One fault's client-visible consequences, per partition.
#[derive(Clone, Debug)]
pub struct FaultOutcome {
    pub fault: &'static str,
    pub victim: usize,
    /// Seconds into the run when the fault became effective (the docker
    /// command returned).
    pub t0_secs: f64,
    /// How long the fault command itself took; t0 is stamped after it.
    pub cli_ms: u64,
    pub heal_after_ms: u64,
    pub partition: u32,
    /// "leader" or "follower" — the victim's role for this partition at t0.
    pub role: &'static str,
    pub leader_moved_ms: Option<u64>,
    pub new_leader: i32,
    pub first_ack_ms: Option<u64>,
    /// Largest interval without acks inside the fault window (fault → acks
    /// resumed after heal, or budget). The client-visible outage.
    pub max_gap_ms: Option<u64>,
}

pub fn outcomes_csv_header() -> &'static str {
    "fault,victim,t0_s,cli_ms,heal_after_ms,partition,role,leader_moved_ms,new_leader,first_ack_ms,max_gap_ms"
}

pub fn outcome_csv_row(o: &FaultOutcome) -> String {
    let opt = |v: Option<u64>| v.map(|x| x.to_string()).unwrap_or_default();
    format!(
        "{},{},{:.3},{},{},{},{},{},{},{},{}",
        o.fault,
        o.victim,
        o.t0_secs,
        o.cli_ms,
        o.heal_after_ms,
        o.partition,
        o.role,
        opt(o.leader_moved_ms),
        o.new_leader,
        opt(o.first_ack_ms),
        opt(o.max_gap_ms)
    )
}

/// Summary statistics over a list of gaps (ms).
pub fn percentiles(mut xs: Vec<u64>) -> Option<(u64, u64, u64, u64, usize)> {
    if xs.is_empty() {
        return None;
    }
    xs.sort_unstable();
    let pct = |q: f64| xs[((xs.len() - 1) as f64 * q).round() as usize];
    Some((xs[0], pct(0.5), pct(0.9), *xs.last().unwrap(), xs.len()))
}
