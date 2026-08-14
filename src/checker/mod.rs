//! Checker v1: verifies the delivery contract from client-observed histories.
//!
//! The test suite doesn't trust the system; it trusts what clients saw. A
//! workload records every *acked* produce (a unique id embedded in the value)
//! and every consumed record; `History::verify` then checks:
//!
//! - **At-least-once:** every acked id was consumed at least once. Violations
//!   here are contract violations, full stop.
//! - **Duplicates are counted, not hidden:** redelivery after a rebalance or
//!   retry is legal and reported honestly.
//! - **Per-consumer offset monotonicity:** within one (consumer, topic,
//!   partition) stream, offsets strictly increase — no reordering, no rewind
//!   the consumer didn't ask for.
//! - **Offset consistency:** if two consumers read the same (topic,
//!   partition, offset), they saw the same id — one log, one truth.
//!
//! Stage 5 grows this into the offline checker fed by on-disk history files
//! from real crashed processes.

use std::collections::{HashMap, HashSet};
use std::fmt;

#[derive(Debug, Clone)]
pub struct ProducedRecord {
    pub id: u64,
    pub topic: String,
}

#[derive(Debug, Clone)]
pub struct ConsumedRecord {
    pub consumer: String,
    /// The consumer's group generation when it read this record. Offsets must
    /// be monotonic *within* one generation; across a rebalance a consumer
    /// legitimately rewinds to the committed offset (that rewind IS
    /// at-least-once redelivery). Standalone consumers pass 0.
    pub generation: u64,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub id: u64,
}

#[derive(Default)]
pub struct History {
    produced: Vec<ProducedRecord>,
    consumed: Vec<ConsumedRecord>,
}

#[derive(Debug)]
pub struct Report {
    pub produced_acked: usize,
    pub consumed_total: usize,
    pub distinct_consumed: usize,
    /// Acked but never consumed: at-least-once violations.
    pub missing_ids: Vec<u64>,
    /// Deliveries beyond the first per id (legal, counted).
    pub duplicate_deliveries: usize,
    pub monotonicity_violations: Vec<String>,
    /// Same (topic, partition, offset) observed with different ids.
    pub divergent_offsets: Vec<String>,
    /// Offsets in `0..=max_consumed` nobody consumed (only checked by
    /// `verify_from_start`, which asserts readers began at offset 0).
    pub offset_gaps: Vec<String>,
}

impl Report {
    pub fn is_ok(&self) -> bool {
        self.missing_ids.is_empty()
            && self.monotonicity_violations.is_empty()
            && self.divergent_offsets.is_empty()
            && self.offset_gaps.is_empty()
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "checker: {} acked, {} consumed ({} distinct), {} duplicate deliveries",
            self.produced_acked, self.consumed_total, self.distinct_consumed,
            self.duplicate_deliveries
        )?;
        if self.missing_ids.is_empty() {
            writeln!(f, "  at-least-once: OK")?;
        } else {
            writeln!(
                f,
                "  at-least-once: VIOLATED — {} acked ids never consumed (first few: {:?})",
                self.missing_ids.len(),
                &self.missing_ids[..self.missing_ids.len().min(10)]
            )?;
        }
        for v in &self.monotonicity_violations {
            writeln!(f, "  monotonicity: VIOLATED — {v}")?;
        }
        for v in &self.divergent_offsets {
            writeln!(f, "  offset consistency: VIOLATED — {v}")?;
        }
        for v in &self.offset_gaps {
            writeln!(f, "  gap-free: VIOLATED — {v}")?;
        }
        Ok(())
    }
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a produce only once its ack (or covering flush ack) arrived.
    pub fn record_produced(&mut self, id: u64, topic: &str) {
        self.produced.push(ProducedRecord {
            id,
            topic: topic.to_string(),
        });
    }

    pub fn record_consumed(
        &mut self,
        consumer: &str,
        generation: u64,
        topic: &str,
        partition: u32,
        offset: u64,
        id: u64,
    ) {
        self.consumed.push(ConsumedRecord {
            consumer: consumer.to_string(),
            generation,
            topic: topic.to_string(),
            partition,
            offset,
            id,
        });
    }

    pub fn produced_ids(&self) -> HashSet<u64> {
        self.produced.iter().map(|p| p.id).collect()
    }

    pub fn consumed_ids(&self) -> HashSet<u64> {
        self.consumed.iter().map(|c| c.id).collect()
    }

    pub fn verify(&self) -> Report {
        let mut consumed_count: HashMap<u64, usize> = HashMap::new();
        for c in &self.consumed {
            *consumed_count.entry(c.id).or_default() += 1;
        }

        let mut missing_ids: Vec<u64> = self
            .produced
            .iter()
            .filter(|p| !consumed_count.contains_key(&p.id))
            .map(|p| p.id)
            .collect();
        missing_ids.sort_unstable();
        missing_ids.dedup();

        let duplicate_deliveries = consumed_count.values().map(|&n| n - 1).sum();

        let mut monotonicity_violations = Vec::new();
        let mut last_seen: HashMap<(&str, u64, &str, u32), u64> = HashMap::new();
        for c in &self.consumed {
            let key = (c.consumer.as_str(), c.generation, c.topic.as_str(), c.partition);
            if let Some(&prev) = last_seen.get(&key)
                && c.offset <= prev
            {
                monotonicity_violations.push(format!(
                    "{} (gen {}) on {}-{}: offset {} after {}",
                    c.consumer, c.generation, c.topic, c.partition, c.offset, prev
                ));
            }
            last_seen.insert(key, c.offset);
        }

        let mut divergent_offsets = Vec::new();
        let mut offset_ids: HashMap<(&str, u32, u64), u64> = HashMap::new();
        for c in &self.consumed {
            let key = (c.topic.as_str(), c.partition, c.offset);
            match offset_ids.get(&key) {
                Some(&id) if id != c.id => divergent_offsets.push(format!(
                    "{}-{} offset {}: id {} vs id {}",
                    c.topic, c.partition, c.offset, id, c.id
                )),
                _ => {
                    offset_ids.insert(key, c.id);
                }
            }
        }

        Report {
            produced_acked: self.produced.len(),
            consumed_total: self.consumed.len(),
            distinct_consumed: consumed_count.len(),
            missing_ids,
            duplicate_deliveries,
            monotonicity_violations,
            divergent_offsets,
            offset_gaps: Vec::new(),
        }
    }

    /// `verify` plus the gap-free check: readers are asserted to have
    /// consumed each partition from offset 0, so every offset in
    /// `0..=max_consumed` must have been consumed by someone — a hole means
    /// the log served non-contiguous history.
    pub fn verify_from_start(&self) -> Report {
        let mut report = self.verify();
        let mut seen: HashMap<(&str, u32), HashSet<u64>> = HashMap::new();
        for c in &self.consumed {
            seen.entry((c.topic.as_str(), c.partition))
                .or_default()
                .insert(c.offset);
        }
        for ((topic, partition), offsets) in seen {
            let max = *offsets.iter().max().unwrap();
            if offsets.len() as u64 != max + 1 {
                let first_hole = (0..=max).find(|o| !offsets.contains(o)).unwrap();
                report.offset_gaps.push(format!(
                    "{}-{}: {} of {} offsets consumed, first hole at {}",
                    topic,
                    partition,
                    offsets.len(),
                    max + 1,
                    first_hole
                ));
            }
        }
        report.offset_gaps.sort();
        report
    }

    /// Persists the history as plain text, one event per line
    /// (`P <id> <topic>` / `C <consumer> <gen> <topic> <partition> <offset>
    /// <id>`), so verification is genuinely offline. Names must not contain
    /// whitespace.
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let mut out = std::io::BufWriter::new(std::fs::File::create(path)?);
        for p in &self.produced {
            debug_assert!(!p.topic.contains(char::is_whitespace));
            writeln!(out, "P {} {}", p.id, p.topic)?;
        }
        for c in &self.consumed {
            debug_assert!(!c.consumer.contains(char::is_whitespace));
            writeln!(
                out,
                "C {} {} {} {} {} {}",
                c.consumer, c.generation, c.topic, c.partition, c.offset, c.id
            )?;
        }
        out.flush()
    }

    pub fn load(path: &std::path::Path) -> std::io::Result<History> {
        use std::io::BufRead;
        let bad = |line: &str| std::io::Error::other(format!("malformed history line: {line}"));
        let mut history = History::new();
        for line in std::io::BufReader::new(std::fs::File::open(path)?).lines() {
            let line = line?;
            let fields: Vec<&str> = line.split_whitespace().collect();
            match fields.as_slice() {
                ["P", id, topic] => {
                    history.record_produced(id.parse().map_err(|_| bad(&line))?, topic)
                }
                ["C", consumer, generation, topic, partition, offset, id] => history
                    .record_consumed(
                        consumer,
                        generation.parse().map_err(|_| bad(&line))?,
                        topic,
                        partition.parse().map_err(|_| bad(&line))?,
                        offset.parse().map_err(|_| bad(&line))?,
                        id.parse().map_err(|_| bad(&line))?,
                    ),
                [] => {}
                _ => return Err(bad(&line)),
            }
        }
        Ok(history)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_history_passes() {
        let mut h = History::new();
        for id in 0..10 {
            h.record_produced(id, "t");
            h.record_consumed("c1", 1, "t", 0, id, id);
        }
        let report = h.verify();
        assert!(report.is_ok(), "{report}");
        assert_eq!(report.duplicate_deliveries, 0);
    }

    #[test]
    fn missing_id_is_a_violation() {
        let mut h = History::new();
        h.record_produced(1, "t");
        h.record_produced(2, "t");
        h.record_consumed("c1", 1, "t", 0, 0, 1);
        let report = h.verify();
        assert!(!report.is_ok());
        assert_eq!(report.missing_ids, vec![2]);
    }

    #[test]
    fn duplicates_are_counted_not_fatal() {
        let mut h = History::new();
        h.record_produced(1, "t");
        h.record_consumed("c1", 1, "t", 0, 0, 1);
        h.record_consumed("c2", 1, "t", 0, 0, 1);
        let report = h.verify();
        assert!(report.is_ok(), "{report}");
        assert_eq!(report.duplicate_deliveries, 1);
    }

    #[test]
    fn rewind_within_generation_and_divergence_are_violations() {
        let mut h = History::new();
        h.record_produced(1, "t");
        h.record_produced(2, "t");
        h.record_consumed("c1", 3, "t", 0, 5, 1);
        h.record_consumed("c1", 3, "t", 0, 5, 1); // same gen, same offset: rewind
        h.record_consumed("c2", 3, "t", 0, 5, 2); // same offset, different id
        let report = h.verify();
        assert_eq!(report.monotonicity_violations.len(), 1);
        assert_eq!(report.divergent_offsets.len(), 1);
    }

    #[test]
    fn rewind_across_generations_is_legal_redelivery() {
        let mut h = History::new();
        h.record_produced(1, "t");
        h.record_consumed("c1", 3, "t", 0, 5, 1);
        h.record_consumed("c1", 4, "t", 0, 5, 1); // rebalance: rewound to commit
        let report = h.verify();
        assert!(report.is_ok(), "{report}");
        assert_eq!(report.duplicate_deliveries, 1);
    }

    #[test]
    fn planted_gap_is_caught_only_by_from_start() {
        let mut h = History::new();
        for offset in [0u64, 1, 3] {
            // offset 2 never consumed
            h.record_produced(offset, "t");
            h.record_consumed("c1", 0, "t", 0, offset, offset);
        }
        assert!(h.verify().offset_gaps.is_empty());
        let report = h.verify_from_start();
        assert_eq!(report.offset_gaps.len(), 1, "{report}");
        assert!(report.offset_gaps[0].contains("first hole at 2"));
        assert!(!report.is_ok());
    }

    #[test]
    fn history_file_roundtrip_preserves_verdict() {
        let mut h = History::new();
        for id in 0..50u64 {
            h.record_produced(id, "t");
            h.record_consumed("reader-a", 0, "t", (id % 2) as u32, id / 2, id);
        }
        h.record_produced(999, "t"); // never consumed: a violation to preserve
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.txt");
        h.save(&path).unwrap();
        let loaded = History::load(&path).unwrap();
        let (a, b) = (h.verify_from_start(), loaded.verify_from_start());
        assert_eq!(a.produced_acked, b.produced_acked);
        assert_eq!(a.consumed_total, b.consumed_total);
        assert_eq!(a.missing_ids, b.missing_ids);
        assert_eq!(a.missing_ids, vec![999]);
        assert_eq!(a.duplicate_deliveries, b.duplicate_deliveries);
        assert_eq!(a.offset_gaps, b.offset_gaps);
    }
}
