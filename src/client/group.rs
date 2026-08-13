//! The group-membership consumer: join → poll/heartbeat → commit, with
//! automatic rejoin when a rebalance leaves this member behind.
//!
//! Heartbeats are poll-driven (like pre-KIP-62 Kafka): a consumer that stops
//! polling stops heartbeating and gets evicted after its session timeout —
//! which is honest, because a stalled consumer isn't making progress anyway.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::client::{ClientError, Connection, Result};
use crate::proto::{ErrorCode, FetchedRecord, Request, Response};

pub struct GroupConsumer {
    conn: Connection,
    group: String,
    topics: Vec<String>,
    session_timeout_ms: u32,
    member_id: String,
    generation: u64,
    assignment: Vec<(String, u32)>,
    positions: HashMap<(String, u32), u64>,
    last_heartbeat: Instant,
    round_robin: usize,
    pub max_bytes: u32,
    pub max_wait_ms: u32,
}

impl GroupConsumer {
    pub async fn join(
        conn: Connection,
        group: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
    ) -> Result<Self> {
        let mut consumer = Self {
            conn,
            group: group.into(),
            topics,
            session_timeout_ms,
            member_id: String::new(),
            generation: 0,
            assignment: Vec::new(),
            positions: HashMap::new(),
            last_heartbeat: Instant::now(),
            round_robin: 0,
            max_bytes: 1 << 20,
            max_wait_ms: 0,
        };
        consumer.rejoin().await?;
        Ok(consumer)
    }

    /// (Re)joins and refreshes assignment + positions from committed offsets.
    async fn rejoin(&mut self) -> Result<()> {
        let resp = self
            .conn
            .call(&Request::JoinGroup {
                group: self.group.clone(),
                member_id: self.member_id.clone(),
                session_timeout_ms: self.session_timeout_ms,
                topics: self.topics.clone(),
            })
            .await?;
        match resp {
            Response::JoinGroup {
                error: ErrorCode::None,
                member_id,
                generation,
                assignment,
            } => {
                let mut positions = HashMap::new();
                for (topic, partition) in &assignment {
                    let committed = self
                        .conn
                        .fetch_offset(&self.group, topic, *partition)
                        .await?;
                    positions.insert((topic.clone(), *partition), committed.unwrap_or(0));
                }
                self.member_id = member_id;
                self.generation = generation;
                self.assignment = assignment;
                self.positions = positions;
                self.last_heartbeat = Instant::now();
                Ok(())
            }
            Response::JoinGroup { error, .. } => Err(ClientError::Broker(error)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    async fn maybe_heartbeat(&mut self) -> Result<()> {
        let interval = Duration::from_millis((self.session_timeout_ms / 3).max(1) as u64);
        if self.last_heartbeat.elapsed() < interval {
            return Ok(());
        }
        let resp = self
            .conn
            .call(&Request::Heartbeat {
                group: self.group.clone(),
                member_id: self.member_id.clone(),
                generation: self.generation,
            })
            .await?;
        match resp {
            Response::Heartbeat {
                error: ErrorCode::None,
            } => {
                self.last_heartbeat = Instant::now();
                Ok(())
            }
            Response::Heartbeat {
                error: ErrorCode::StaleGeneration | ErrorCode::UnknownMember,
            } => self.rejoin().await,
            Response::Heartbeat { error } => Err(ClientError::Broker(error)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Heartbeats if due, then fetches from assigned partitions round-robin.
    /// Returns the first non-empty batch as (topic, partition, record)s, or
    /// empty if nothing is available anywhere right now.
    pub async fn poll(&mut self) -> Result<Vec<(String, u32, FetchedRecord)>> {
        self.maybe_heartbeat().await?;
        if self.assignment.is_empty() {
            return Ok(Vec::new());
        }
        for _ in 0..self.assignment.len() {
            let (topic, partition) =
                self.assignment[self.round_robin % self.assignment.len()].clone();
            self.round_robin += 1;
            let position = *self.positions.get(&(topic.clone(), partition)).unwrap_or(&0);
            let (_, _, records) = self
                .conn
                .fetch(&topic, partition, position, self.max_bytes, self.max_wait_ms)
                .await?;
            if let Some(last) = records.last() {
                self.positions
                    .insert((topic.clone(), partition), last.offset + 1);
                return Ok(records
                    .into_iter()
                    .map(|r| (topic.clone(), partition, r))
                    .collect());
            }
        }
        Ok(Vec::new())
    }

    /// Commits every assigned partition's position, fenced by (member_id,
    /// generation). If the group rebalanced under us, rejoins and reports
    /// StaleGeneration — records consumed since the last successful commit
    /// will be redelivered to their new owner (at-least-once, by design).
    pub async fn commit(&mut self) -> Result<()> {
        for ((topic, partition), position) in self.positions.clone() {
            let result = self
                .conn
                .commit_offset_fenced(
                    &self.group,
                    &topic,
                    partition,
                    position,
                    &self.member_id,
                    self.generation,
                )
                .await;
            match result {
                Ok(()) => {}
                Err(ClientError::Broker(
                    code @ (ErrorCode::StaleGeneration | ErrorCode::UnknownMember),
                )) => {
                    self.rejoin().await?;
                    return Err(ClientError::Broker(code));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn assignment(&self) -> &[(String, u32)] {
        &self.assignment
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}
