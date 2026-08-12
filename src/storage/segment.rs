use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::storage::index::SparseIndex;
use crate::storage::record::{DecodeOutcome, Record};
use crate::storage::{LogConfig, Result, StorageError, fsync_file};

pub struct Segment {
    base_offset: u64,
    log_path: PathBuf,
    writer: File,
    index: SparseIndex,
    next_offset: u64,
    size: u64,
    bytes_since_index: u64,
}

fn log_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.log"))
}

fn index_path(dir: &Path, base_offset: u64) -> PathBuf {
    dir.join(format!("{base_offset:020}.index"))
}

impl Segment {
    pub fn create(dir: &Path, base_offset: u64) -> Result<Self> {
        let lp = log_path(dir, base_offset);
        let writer = OpenOptions::new().create_new(true).append(true).open(&lp)?;
        let index = SparseIndex::create(&index_path(dir, base_offset))?;
        Ok(Self {
            base_offset,
            log_path: lp,
            writer,
            index,
            next_offset: base_offset,
            size: 0,
            bytes_since_index: 0,
        })
    }

    /// Reopens a segment, validating its contents.
    ///
    /// `truncate_torn_tail` is true only for the newest segment: a crash can
    /// tear a write only where writes were happening, so an invalid tail there
    /// is truncated. In a rolled segment (fsynced before the roll), an invalid
    /// record is real corruption and must surface as an error — truncating it
    /// would silently destroy acknowledged data.
    pub fn recover(
        dir: &Path,
        base_offset: u64,
        config: &LogConfig,
        truncate_torn_tail: bool,
    ) -> Result<Self> {
        let lp = log_path(dir, base_offset);
        let ip = index_path(dir, base_offset);

        // The active segment always rebuilds its index: recovery may truncate
        // the log, and a derived index must never outlive the bytes it maps.
        let (mut index, scan_pos, scan_offset, rebuild) = if truncate_torn_tail {
            (SparseIndex::create(&ip)?, 0u64, base_offset, true)
        } else {
            match SparseIndex::load(&ip)? {
                Some(idx) => match idx.last() {
                    Some((rel, pos)) => (idx, pos as u64, base_offset + rel as u64, false),
                    None => (idx, 0, base_offset, false),
                },
                None => (SparseIndex::create(&ip)?, 0, base_offset, true),
            }
        };

        let mut file = File::open(&lp)?;
        let file_len = file.metadata()?.len();
        file.seek(SeekFrom::Start(scan_pos))?;
        let mut buf = Vec::with_capacity((file_len - scan_pos) as usize);
        file.read_to_end(&mut buf)?;

        let mut pos_in_buf = 0usize;
        let mut next_offset = scan_offset;
        let mut bytes_since_index = 0u64;
        let mut valid_end = scan_pos;
        loop {
            match Record::decode(&buf[pos_in_buf..], config.max_record_bytes) {
                DecodeOutcome::Record { record, consumed } => {
                    if record.offset != next_offset {
                        return Err(StorageError::Corrupt {
                            offset: record.offset,
                            position: valid_end,
                            reason: format!("offset discontinuity: expected {next_offset}"),
                        });
                    }
                    if rebuild && bytes_since_index >= config.index_interval_bytes {
                        let rel = (record.offset - base_offset) as u32;
                        index.append(rel, valid_end as u32)?;
                        bytes_since_index = 0;
                    }
                    bytes_since_index += consumed as u64;
                    next_offset += 1;
                    pos_in_buf += consumed;
                    valid_end += consumed as u64;
                }
                DecodeOutcome::Incomplete => {
                    if pos_in_buf == buf.len() {
                        break;
                    }
                    if truncate_torn_tail {
                        break;
                    }
                    return Err(StorageError::Corrupt {
                        offset: next_offset,
                        position: valid_end,
                        reason: "incomplete record in rolled segment".into(),
                    });
                }
                DecodeOutcome::Corrupt(reason) => {
                    if truncate_torn_tail {
                        break;
                    }
                    return Err(StorageError::Corrupt {
                        offset: next_offset,
                        position: valid_end,
                        reason,
                    });
                }
            }
        }

        if valid_end < file_len {
            let f = OpenOptions::new().write(true).open(&lp)?;
            f.set_len(valid_end)?;
            fsync_file(&f)?;
        }

        let writer = OpenOptions::new().append(true).open(&lp)?;
        Ok(Self {
            base_offset,
            log_path: lp,
            writer,
            index,
            next_offset,
            size: valid_end,
            bytes_since_index,
        })
    }

    /// Appends an encoded record; returns bytes written. The caller (the log)
    /// assigns offsets and decides when to fsync.
    pub fn append(&mut self, record: &Record, config: &LogConfig) -> Result<u64> {
        debug_assert_eq!(record.offset, self.next_offset);
        let mut buf = Vec::with_capacity(record.encoded_len());
        record.encode(&mut buf);
        if self.bytes_since_index >= config.index_interval_bytes {
            let rel = (record.offset - self.base_offset) as u32;
            self.index.append(rel, self.size as u32)?;
            self.bytes_since_index = 0;
        }
        use std::io::Write;
        self.writer.write_all(&buf)?;
        self.size += buf.len() as u64;
        self.bytes_since_index += buf.len() as u64;
        self.next_offset += 1;
        Ok(buf.len() as u64)
    }

    pub fn flush(&mut self) -> Result<()> {
        fsync_file(&self.writer)?;
        Ok(())
    }

    /// Reads records starting at `offset` (which must lie in this segment),
    /// up to roughly `max_bytes` of encoded records — always at least one.
    pub fn read_from(&self, offset: u64, max_bytes: u64, config: &LogConfig) -> Result<Vec<Record>> {
        if offset < self.base_offset || offset >= self.next_offset {
            return Ok(Vec::new());
        }
        let rel = (offset - self.base_offset) as u32;
        let start_pos = self.index.lookup(rel).map(|(_, pos)| pos as u64).unwrap_or(0);
        let mut file = File::open(&self.log_path)?;
        file.seek(SeekFrom::Start(start_pos))?;
        let mut buf = Vec::new();
        // Cap the read at the committed size: a fresh handle would otherwise
        // see bytes an in-flight writer appended after this snapshot.
        file.take(self.size - start_pos).read_to_end(&mut buf)?;

        let mut out = Vec::new();
        let mut pos = 0usize;
        let mut out_bytes = 0u64;
        loop {
            match Record::decode(&buf[pos..], config.max_record_bytes) {
                DecodeOutcome::Record { record, consumed } => {
                    pos += consumed;
                    if record.offset < offset {
                        continue;
                    }
                    if !out.is_empty() && out_bytes + consumed as u64 > max_bytes {
                        break;
                    }
                    out_bytes += consumed as u64;
                    out.push(record);
                    if out_bytes >= max_bytes {
                        break;
                    }
                }
                DecodeOutcome::Incomplete => break,
                DecodeOutcome::Corrupt(reason) => {
                    return Err(StorageError::Corrupt {
                        offset: self.next_offset,
                        position: start_pos + pos as u64,
                        reason,
                    });
                }
            }
        }
        Ok(out)
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}
