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
        let offset = self.active.next_offset();
        let record = Record {
            offset,
            timestamp_ms: Record::now_ms(),
            key,
            value,
        };
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
}
