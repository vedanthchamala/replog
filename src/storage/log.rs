use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::storage::record::Record;
use crate::storage::segment::Segment;
use crate::storage::{FsyncPolicy, LogConfig, Result, StorageError};

/// One partition's log: a directory of segments, exactly one of them active.
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    closed: BTreeMap<u64, Segment>,
    active: Segment,
    durable_offset: Option<u64>,
    dirty_bytes: u64,
    last_flush: Instant,
}

#[derive(Debug, Clone, Copy)]
pub struct AppendInfo {
    pub offset: u64,
    /// Highest offset known to be on stable storage after this append.
    /// Under `Batch`/`Os` policies this trails `offset`; the broker must not
    /// ack `acks=all`-style writes past it.
    pub durable_offset: Option<u64>,
}

impl Log {
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Log> {
        config.validate()?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut bases: Vec<u64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".log") {
                let base = stem.parse::<u64>().map_err(|_| {
                    StorageError::InvalidLayout(format!("unparseable segment file name {name}"))
                })?;
                bases.push(base);
            }
        }
        bases.sort_unstable();

        if bases.is_empty() {
            let active = Segment::create(&dir, 0)?;
            return Ok(Log {
                dir,
                config,
                closed: BTreeMap::new(),
                active,
                durable_offset: None,
                dirty_bytes: 0,
                last_flush: Instant::now(),
            });
        }

        let last = *bases.last().unwrap();
        let mut closed = BTreeMap::new();
        let mut active: Option<Segment> = None;
        let mut expected_base: Option<u64> = None;
        for &base in &bases {
            let is_last = base == last;
            let seg = Segment::recover(&dir, base, &config, is_last)?;
            if let Some(expected) = expected_base
                && base != expected {
                    return Err(StorageError::InvalidLayout(format!(
                        "segment base {base} does not continue previous segment (expected {expected})"
                    )));
                }
            expected_base = Some(seg.next_offset());
            if is_last {
                active = Some(seg);
            } else {
                closed.insert(base, seg);
            }
        }
        let active = active.expect("last segment always recovered");
        // Everything that survived recovery is on disk by definition; treat it
        // as the recovery point.
        let durable_offset = active.next_offset().checked_sub(1);
        Ok(Log {
            dir,
            config,
            closed,
            active,
            durable_offset,
            dirty_bytes: 0,
            last_flush: Instant::now(),
        })
    }

    pub fn append(&mut self, key: Option<Vec<u8>>, value: Vec<u8>) -> Result<AppendInfo> {
        let record = Record {
            offset: self.active.next_offset(),
            timestamp_ms: Record::now_ms(),
            key,
            value,
        };
        self.append_record(record)
    }

    /// Appends a record fetched from a leader, preserving its offset and
    /// timestamp. The offset must continue this log exactly — replication is
    /// not allowed to create gaps or rewrites.
    pub fn append_replicated(&mut self, record: Record) -> Result<AppendInfo> {
        if record.offset != self.next_offset() {
            return Err(StorageError::InvalidLayout(format!(
                "replicated append at offset {} but log ends at {}",
                record.offset,
                self.next_offset()
            )));
        }
        self.append_record(record)
    }

    fn append_record(&mut self, record: Record) -> Result<AppendInfo> {
        let offset = record.offset;
        let encoded_len = record.encoded_len() as u64;
        if encoded_len > self.config.max_record_bytes as u64 {
            return Err(StorageError::RecordTooLarge(encoded_len));
        }
        if self.active.size() > 0 && self.active.size() + encoded_len > self.config.max_segment_bytes
        {
            self.roll()?;
        }
        let written = self.active.append(&record, &self.config)?;
        self.dirty_bytes += written;
        match self.config.fsync {
            FsyncPolicy::Always => {
                self.flush()?;
            }
            FsyncPolicy::Batch { max_bytes, max_ms } => {
                if self.dirty_bytes >= max_bytes
                    || self.last_flush.elapsed().as_millis() as u64 >= max_ms
                {
                    self.flush()?;
                }
            }
            FsyncPolicy::Os => {}
        }
        Ok(AppendInfo {
            offset,
            durable_offset: self.durable_offset,
        })
    }

    /// Forces everything appended so far to stable storage; returns the new
    /// durable offset.
    pub fn flush(&mut self) -> Result<Option<u64>> {
        self.active.flush()?;
        self.durable_offset = self.active.next_offset().checked_sub(1);
        self.dirty_bytes = 0;
        self.last_flush = Instant::now();
        Ok(self.durable_offset)
    }

    /// Reads records starting at `offset`, up to roughly `max_bytes` (always
    /// at least one if any exist). A read never crosses a segment boundary;
    /// callers continue from the next offset, consumer-style.
    pub fn read(&self, offset: u64, max_bytes: u64) -> Result<Vec<Record>> {
        if offset < self.start_offset() || offset > self.next_offset() {
            return Err(StorageError::OffsetOutOfRange(offset));
        }
        if offset == self.next_offset() {
            return Ok(Vec::new());
        }
        if offset >= self.active.base_offset() {
            return self.active.read_from(offset, max_bytes, &self.config);
        }
        let (_, seg) = self
            .closed
            .range(..=offset)
            .next_back()
            .expect("in-range offset must land in a segment");
        seg.read_from(offset, max_bytes, &self.config)
    }

    /// Rolling fsyncs the outgoing segment first: recovery trusts rolled
    /// segments without a torn-tail scan, so they must be durable before the
    /// new segment exists.
    fn roll(&mut self) -> Result<()> {
        self.flush()?;
        let next_base = self.active.next_offset();
        let new_segment = Segment::create(&self.dir, next_base)?;
        let old = std::mem::replace(&mut self.active, new_segment);
        self.closed.insert(old.base_offset(), old);
        Ok(())
    }

    pub fn start_offset(&self) -> u64 {
        self.closed
            .keys()
            .next()
            .copied()
            .unwrap_or_else(|| self.active.base_offset())
    }

    pub fn next_offset(&self) -> u64 {
        self.active.next_offset()
    }

    pub fn durable_offset(&self) -> Option<u64> {
        self.durable_offset
    }

    pub fn segment_count(&self) -> usize {
        self.closed.len() + 1
    }

    /// Truncates the log so that `new_next` becomes the next offset (records
    /// at `new_next` and beyond are removed). Tail-only by construction: this
    /// exists for a follower reconciling its divergent suffix with a new
    /// leader, never for editing history.
    pub fn truncate_suffix(&mut self, new_next: u64) -> Result<()> {
        use crate::storage::segment::{index_path, log_path};

        if new_next >= self.next_offset() {
            return Ok(());
        }
        if new_next < self.start_offset() {
            return Err(StorageError::OffsetOutOfRange(new_next));
        }

        // The segment that keeps the tail: the one covering new_next - 1, or
        // the very first segment when the whole log is being emptied.
        let target_base = if new_next == self.start_offset() {
            self.start_offset()
        } else if self.active.base_offset() < new_next {
            self.active.base_offset()
        } else {
            *self
                .closed
                .range(..new_next)
                .next_back()
                .map(|(base, _)| base)
                .expect("a segment must cover offsets before new_next")
        };

        let doomed: Vec<u64> = self
            .closed
            .range((
                std::ops::Bound::Excluded(target_base),
                std::ops::Bound::Unbounded,
            ))
            .map(|(base, _)| *base)
            .collect();
        for base in doomed {
            self.closed.remove(&base);
            fs::remove_file(log_path(&self.dir, base))?;
            fs::remove_file(index_path(&self.dir, base))?;
        }
        let target_is_active = self.active.base_offset() == target_base;
        if !target_is_active {
            fs::remove_file(log_path(&self.dir, self.active.base_offset()))?;
            fs::remove_file(index_path(&self.dir, self.active.base_offset()))?;
        }

        let position = if target_is_active {
            self.active.position_of(new_next, &self.config)?
        } else {
            self.closed
                .get(&target_base)
                .expect("target segment present")
                .position_of(new_next, &self.config)?
        };
        let target_log = log_path(&self.dir, target_base);
        let file = fs::OpenOptions::new().write(true).open(&target_log)?;
        file.set_len(position)?;
        crate::storage::fsync_file(&file)?;

        let rebuilt = Segment::recover(&self.dir, target_base, &self.config, true)?;
        self.closed.remove(&target_base);
        self.active = rebuilt;

        self.durable_offset = match (self.durable_offset, new_next.checked_sub(1)) {
            (Some(durable), Some(last)) => Some(durable.min(last)),
            _ => None,
        };
        Ok(())
    }
}
