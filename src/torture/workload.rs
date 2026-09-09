//! Workload adapters: how the harness speaks a target's client protocol.
//!
//! The contract the checker verifies is stated in terms of what clients saw,
//! so the only thing an adapter owes the harness is honest bookkeeping: an id
//! counts as *acked* only when the client library reported success at the
//! requested ack level, and a reader reports exactly the `(offset, id)` pairs
//! it received, in the order it received them.

use std::future::Future;

/// Ack level of a produce, in each system's own vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckLevel {
    /// replog `Acks::Written` / Kafka `acks=1`: the leader alone has it.
    /// Outside the durability contract — used only as a sensitivity control.
    LeaderOnly,
    /// replog `Acks::All` / Kafka `acks=all`: the contract-level ack.
    All,
}

impl AckLevel {
    pub fn name(self) -> &'static str {
        match self {
            AckLevel::LeaderOnly => "acks=1",
            AckLevel::All => "acks=all",
        }
    }

    pub fn parse(s: &str) -> Result<AckLevel, String> {
        match s {
            "1" | "leader" | "acks=1" => Ok(AckLevel::LeaderOnly),
            "all" | "acks=all" => Ok(AckLevel::All),
            other => Err(format!("unknown ack level {other:?} (1|all)")),
        }
    }
}

/// Value layout shared by every adapter: the id in the first 8 bytes, little
/// endian, zero padding up to `value_bytes`.
pub fn encode_value(id: u64, value_bytes: usize) -> Vec<u8> {
    let mut v = vec![0u8; value_bytes.max(8)];
    v[..8].copy_from_slice(&id.to_le_bytes());
    v
}

pub fn decode_id(value: &[u8]) -> Option<u64> {
    value.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}

pub trait Workload: Send + Sync + 'static {
    type Reader: Reader;

    /// Produce `ids` to `partition` at `acks`; returns the ids the client
    /// library confirmed. All-or-nothing for replog's batch produce,
    /// per-record for Kafka delivery reports. Failure means "nothing acked" —
    /// the contract says nothing about unacked ids in either direction.
    fn produce(
        &self,
        topic: &str,
        partition: u32,
        ids: &[u64],
        acks: AckLevel,
    ) -> impl Future<Output = Vec<u64>> + Send;

    /// A sequential reader positioned at offset 0 of `partition`.
    fn reader(
        &self,
        topic: &str,
        partition: u32,
        name: &str,
    ) -> impl Future<Output = Result<Self::Reader, String>> + Send;
}

pub trait Reader: Send + 'static {
    /// The next `(offset, id)` pairs, in arrival order; empty if nothing
    /// arrived within the adapter's poll interval. `Err` is transient — the
    /// caller backs off and calls again.
    fn next(&mut self) -> impl Future<Output = Result<Vec<(u64, u64)>, String>> + Send;
}
