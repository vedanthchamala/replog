//! Consumer offsets stored in an internal `__offsets` log — the log dogfooding
//! itself as its own metadata store, as Kafka does with `__consumer_offsets`.
//!
//! A commit is an append of (group, topic, partition) → offset; the latest
//! record for a key wins. At startup the whole log is replayed into an
//! in-memory map. No compaction yet (Stage 6 stretch), so the log only grows.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::broker::partition::PartitionHandle;
use crate::proto::wire::{self, Reader};
use crate::proto::{Acks, ErrorCode};

pub const OFFSETS_TOPIC: &str = "__offsets";

pub struct OffsetsStore {
    partition: PartitionHandle,
    cache: RwLock<HashMap<(String, String, u32), u64>>,
}

fn encode_key(group: &str, topic: &str, partition: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(group.len() + topic.len() + 8);
    wire::put_str(&mut key, group);
    wire::put_str(&mut key, topic);
    wire::put_u32(&mut key, partition);
    key
}

fn decode_entry(key: &[u8], value: &[u8]) -> Option<((String, String, u32), u64)> {
    let mut r = Reader::new(key);
    let group = r.string().ok()?;
    let topic = r.string().ok()?;
    let partition = r.u32().ok()?;
    r.expect_end().ok()?;
    let offset = u64::from_le_bytes(value.try_into().ok()?);
    Some(((group, topic, partition), offset))
}

impl OffsetsStore {
    /// Replays the offsets log into memory.
    pub async fn open(partition: PartitionHandle) -> Result<Self, ErrorCode> {
        let mut cache = HashMap::new();
        let mut pos = 0u64;
        loop {
            let ok = partition.read(pos, 4 * 1024 * 1024).await?;
            if ok.records.is_empty() {
                break;
            }
            for rec in &ok.records {
                pos = rec.offset + 1;
                let Some(key) = rec.key.as_deref() else {
                    eprintln!("__offsets record {} has no key; skipping", rec.offset);
                    continue;
                };
                match decode_entry(key, &rec.value) {
                    Some((k, v)) => {
                        cache.insert(k, v);
                    }
                    None => eprintln!("__offsets record {} malformed; skipping", rec.offset),
                }
            }
        }
        Ok(Self {
            partition,
            cache: RwLock::new(cache),
        })
    }

    pub async fn commit(&self, group: String, topic: String, partition: u32, offset: u64) -> ErrorCode {
        let key = encode_key(&group, &topic, partition);
        let value = offset.to_le_bytes().to_vec();
        match self
            .partition
            .append(vec![(Some(key), value)], Acks::Durable)
            .await
        {
            Ok(_) => {
                self.cache
                    .write()
                    .unwrap()
                    .insert((group, topic, partition), offset);
                ErrorCode::None
            }
            Err(code) => code,
        }
    }

    pub fn fetch(&self, group: &str, topic: &str, partition: u32) -> Option<u64> {
        self.cache
            .read()
            .unwrap()
            .get(&(group.to_string(), topic.to_string(), partition))
            .copied()
    }
}
