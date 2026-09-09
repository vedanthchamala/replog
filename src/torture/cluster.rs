//! The cluster under test, as the harness is allowed to see it.
//!
//! Everything here is client-observable on purpose. Leadership is read through
//! the target's *metadata API*, never from server internals, so "the leader
//! changed at t1" means the same thing for replog and for a Kafka-API system
//! and the failover split (detection + election vs. client recovery) is
//! comparable across them.

use std::future::Future;
use std::time::Duration;

/// A fault the backend can apply to one broker. Sequential, one at a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fault {
    /// SIGKILL the broker process; heal = restart on the same data.
    Kill,
    /// Freeze the process (cgroup freezer / SIGSTOP): sockets stay open,
    /// nothing progresses. Only timeouts can notice — TCP never resets.
    Pause,
    /// Disconnect from the peers network only. Clients still reach the
    /// broker; its peers and the control plane lose it: a live zombie leader.
    Isolate,
}

impl Fault {
    pub const ALL: [Fault; 3] = [Fault::Kill, Fault::Pause, Fault::Isolate];

    pub fn name(self) -> &'static str {
        match self {
            Fault::Kill => "kill",
            Fault::Pause => "pause",
            Fault::Isolate => "isolate",
        }
    }

    pub fn parse(s: &str) -> Result<Fault, String> {
        match s.trim() {
            "kill" => Ok(Fault::Kill),
            "pause" => Ok(Fault::Pause),
            "isolate" => Ok(Fault::Isolate),
            other => Err(format!("unknown fault {other:?} (kill|pause|isolate)")),
        }
    }

    /// `"kill,pause,isolate"` → the list, in order, duplicates allowed (they
    /// weight the draw).
    pub fn parse_list(s: &str) -> Result<Vec<Fault>, String> {
        let list: Result<Vec<_>, _> = s.split(',').filter(|p| !p.is_empty()).map(Fault::parse).collect();
        let list = list?;
        if list.is_empty() {
            return Err("fault list is empty".into());
        }
        Ok(list)
    }
}

/// What the schedule needs from a cluster. Broker `i` is an index into the
/// target's fixed broker list; leaders are reported in the same index space
/// (`-1` = no leader / unknown).
pub trait FaultTarget: Send + Sync {
    fn broker_count(&self) -> usize;
    fn broker_name(&self, i: usize) -> String;
    /// Client-facing address of broker `i`, for probing the victim directly.
    fn client_addr(&self, i: usize) -> String;

    fn fault(&self, i: usize, f: Fault) -> impl Future<Output = Result<(), String>> + Send;
    fn heal(&self, i: usize, f: Fault) -> impl Future<Output = Result<(), String>> + Send;

    fn create_topic(
        &self,
        topic: &str,
        partitions: u32,
        replication: u32,
    ) -> impl Future<Output = Result<(), String>> + Send;

    /// Leader per partition *as broker `i` itself reports it*, asked directly
    /// over its own client port with a short timeout. On an isolated broker
    /// this is the zombie's own (stale) opinion of the world; `-1` = no leader.
    fn view_via(
        &self,
        i: usize,
        topic: &str,
        partitions: u32,
    ) -> impl Future<Output = Result<Vec<i32>, String>> + Send;

    /// Block until every partition is fully replicated again (full ISR, or
    /// no under-replicated partitions) and every broker is a member.
    fn wait_healthy(
        &self,
        topic: &str,
        partitions: u32,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Every broker's view at once (None = did not answer within the timeout).
pub async fn views<T: FaultTarget>(target: &T, topic: &str, partitions: u32) -> Vec<Option<Vec<i32>>> {
    let n = target.broker_count();
    let mut futs: Vec<std::pin::Pin<Box<dyn Future<Output = Result<Vec<i32>, String>> + Send + '_>>> =
        (0..n).map(|i| Box::pin(target.view_via(i, topic, partitions)) as _).collect();
    let mut out: Vec<Option<Option<Vec<i32>>>> = (0..n).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut done = true;
        for (i, f) in futs.iter_mut().enumerate() {
            if out[i].is_none() {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(r) => out[i] = Some(r.ok()),
                    std::task::Poll::Pending => done = false,
                }
            }
        }
        if done { std::task::Poll::Ready(()) } else { std::task::Poll::Pending }
    })
    .await;
    out.into_iter().map(|o| o.unwrap()).collect()
}

/// The cluster's opinion: per partition, the leader named by a strict
/// majority of *all* brokers (answering or not); `-1` when there is none.
/// A lone zombie's stale claim can never win this vote, and a client that
/// refreshes from any two live brokers would learn the same answer.
pub fn majority(views: &[Option<Vec<i32>>], partitions: u32) -> Vec<i32> {
    let n = views.len();
    (0..partitions as usize)
        .map(|p| {
            let mut counts: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();
            for v in views.iter().flatten() {
                if let Some(&l) = v.get(p) {
                    if l >= 0 {
                        *counts.entry(l).or_default() += 1;
                    }
                }
            }
            counts
                .into_iter()
                .find(|&(_, c)| c * 2 > n)
                .map(|(l, _)| l)
                .unwrap_or(-1)
        })
        .collect()
}

pub async fn majority_leaders<T: FaultTarget>(target: &T, topic: &str, partitions: u32) -> Vec<i32> {
    majority(&views(target, topic, partitions).await, partitions)
}
