//! The group coordinator: membership, generations, range assignment, and
//! zombie fencing for one broker.
//!
//! Simplification vs Kafka (documented in PLAN.md): the broker computes
//! assignments itself instead of shipping the member list to a client-side
//! "leader" (Kafka's JoinGroup/SyncGroup two-phase exists so assignment
//! strategies are pluggable without broker upgrades; replog needs one
//! strategy, so one round trip and one owner of truth).
//!
//! Every membership change bumps the group's generation. A member whose
//! requests carry an old generation is a zombie — it missed a rebalance —
//! and gets StaleGeneration, whose only correct handling is to rejoin.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::proto::ErrorCode;

struct Member {
    topics: Vec<String>,
    session_timeout: Duration,
    last_heartbeat: Instant,
}

#[derive(Default)]
struct Group {
    generation: u64,
    members: HashMap<String, Member>,
    assignment: HashMap<String, Vec<(String, u32)>>,
}

pub struct JoinOutcome {
    pub member_id: String,
    pub generation: u64,
    pub assignment: Vec<(String, u32)>,
}

pub struct GroupCoordinator {
    groups: Mutex<HashMap<String, Group>>,
    next_member: AtomicU64,
}

/// Range assignment: for each topic, its partitions in order are chunked
/// across the subscribed members in member-id order, remainder to the front.
fn assign(
    members: &HashMap<String, Member>,
    partition_counts: &dyn Fn(&str) -> Option<u32>,
) -> HashMap<String, Vec<(String, u32)>> {
    let mut out: HashMap<String, Vec<(String, u32)>> =
        members.keys().map(|id| (id.clone(), Vec::new())).collect();
    let mut topics: Vec<&String> = members.values().flat_map(|m| m.topics.iter()).collect();
    topics.sort();
    topics.dedup();
    for topic in topics {
        let Some(partitions) = partition_counts(topic) else {
            continue;
        };
        let mut subscribed: Vec<&String> = members
            .iter()
            .filter(|(_, m)| m.topics.contains(topic))
            .map(|(id, _)| id)
            .collect();
        subscribed.sort();
        if subscribed.is_empty() {
            continue;
        }
        let n = subscribed.len() as u32;
        let per = partitions / n;
        let extra = partitions % n;
        let mut next = 0u32;
        for (i, member) in subscribed.iter().enumerate() {
            let take = per + if (i as u32) < extra { 1 } else { 0 };
            let slots = out.get_mut(*member).unwrap();
            for p in next..next + take {
                slots.push((topic.clone(), p));
            }
            next += take;
        }
    }
    out
}

impl GroupCoordinator {
    pub fn new() -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            next_member: AtomicU64::new(1),
        }
    }

    /// Joins (or rejoins) a group. A new member bumps the generation and
    /// triggers reassignment; a rejoin returns the current state unchanged
    /// unless its subscriptions changed.
    pub fn join(
        &self,
        group: &str,
        member_id: &str,
        session_timeout_ms: u32,
        topics: Vec<String>,
        partition_counts: &dyn Fn(&str) -> Option<u32>,
    ) -> JoinOutcome {
        let mut groups = self.groups.lock().unwrap();
        let g = groups.entry(group.to_string()).or_default();
        let (member_id, changed) = if member_id.is_empty() || !g.members.contains_key(member_id) {
            let id = format!("m-{}", self.next_member.fetch_add(1, Ordering::Relaxed));
            g.members.insert(
                id.clone(),
                Member {
                    topics,
                    session_timeout: Duration::from_millis(session_timeout_ms as u64),
                    last_heartbeat: Instant::now(),
                },
            );
            (id, true)
        } else {
            let m = g.members.get_mut(member_id).unwrap();
            m.last_heartbeat = Instant::now();
            m.session_timeout = Duration::from_millis(session_timeout_ms as u64);
            let changed = m.topics != topics;
            m.topics = topics;
            (member_id.to_string(), changed)
        };
        if changed {
            g.generation += 1;
            g.assignment = assign(&g.members, partition_counts);
        }
        JoinOutcome {
            assignment: g.assignment.get(&member_id).cloned().unwrap_or_default(),
            generation: g.generation,
            member_id,
        }
    }

    pub fn heartbeat(&self, group: &str, member_id: &str, generation: u64) -> ErrorCode {
        let mut groups = self.groups.lock().unwrap();
        let Some(g) = groups.get_mut(group) else {
            return ErrorCode::UnknownMember;
        };
        let Some(m) = g.members.get_mut(member_id) else {
            return ErrorCode::UnknownMember;
        };
        m.last_heartbeat = Instant::now();
        if generation != g.generation {
            return ErrorCode::StaleGeneration;
        }
        ErrorCode::None
    }

    pub fn leave(
        &self,
        group: &str,
        member_id: &str,
        partition_counts: &dyn Fn(&str) -> Option<u32>,
    ) -> ErrorCode {
        let mut groups = self.groups.lock().unwrap();
        let Some(g) = groups.get_mut(group) else {
            return ErrorCode::UnknownMember;
        };
        if g.members.remove(member_id).is_none() {
            return ErrorCode::UnknownMember;
        }
        g.generation += 1;
        g.assignment = assign(&g.members, partition_counts);
        ErrorCode::None
    }

    /// Validates a fenced commit. Empty member_id = unfenced, always allowed.
    pub fn check_commit(&self, group: &str, member_id: &str, generation: u64) -> ErrorCode {
        if member_id.is_empty() {
            return ErrorCode::None;
        }
        let groups = self.groups.lock().unwrap();
        let Some(g) = groups.get(group) else {
            return ErrorCode::UnknownMember;
        };
        if !g.members.contains_key(member_id) {
            return ErrorCode::UnknownMember;
        }
        if generation != g.generation {
            return ErrorCode::StaleGeneration;
        }
        ErrorCode::None
    }

    /// Evicts members silent past their session timeout; returns how many
    /// were evicted. Crashed consumers (no Leave) are detected here.
    pub fn expire(&self, partition_counts: &dyn Fn(&str) -> Option<u32>) -> usize {
        let mut groups = self.groups.lock().unwrap();
        let now = Instant::now();
        let mut evicted = 0;
        for g in groups.values_mut() {
            let dead: Vec<String> = g
                .members
                .iter()
                .filter(|(_, m)| now.duration_since(m.last_heartbeat) > m.session_timeout)
                .map(|(id, _)| id.clone())
                .collect();
            if dead.is_empty() {
                continue;
            }
            for id in &dead {
                g.members.remove(id);
                evicted += 1;
            }
            g.generation += 1;
            g.assignment = assign(&g.members, partition_counts);
        }
        evicted
    }
}
