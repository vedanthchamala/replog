//! Leader-epoch checkpoint: per-partition history of (epoch, start_offset)
//! pairs, appended when a replica becomes leader for a new epoch.
//!
//! This is what makes divergence detection possible (KIP-101's insight): a
//! follower reports the last epoch in its history and the leader answers with
//! where that epoch *ended* in the leader's history; the follower truncates to
//! the smaller of the two. Offsets alone cannot distinguish "same history,
//! shorter" from "different history, same length" — epochs can.
//!
//! Unlike the sparse index this file is NOT derived state (it encodes history
//! that the log bytes alone don't), so it is fsynced on append and a malformed
//! file is a hard error, not a rebuild.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::storage::fsync_file;

const FILE_NAME: &str = "leader-epochs";
const ENTRY_BYTES: usize = 16;

pub struct EpochCheckpoint {
    path: PathBuf,
    entries: Vec<(u64, u64)>,
}

impl EpochCheckpoint {
    pub fn load(dir: &Path) -> io::Result<Self> {
        let path = dir.join(FILE_NAME);
        let mut entries = Vec::new();
        match File::open(&path) {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                if bytes.len() % ENTRY_BYTES != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "leader-epochs file has a partial entry",
                    ));
                }
                for chunk in bytes.chunks_exact(ENTRY_BYTES) {
                    let epoch = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
                    let start = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
                    if let Some(&(last_epoch, last_start)) = entries.last()
                        && (epoch <= last_epoch || start < last_start)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "leader-epochs entries are not monotonic",
                        ));
                    }
                    entries.push((epoch, start));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(Self { path, entries })
    }

    pub fn entries(&self) -> &[(u64, u64)] {
        &self.entries
    }

    pub fn last(&self) -> Option<(u64, u64)> {
        self.entries.last().copied()
    }

    pub fn current_epoch(&self) -> u64 {
        self.entries.last().map_or(0, |&(epoch, _)| epoch)
    }

    /// Records that `epoch` starts at `start_offset`. No-op if it is already
    /// the newest entry.
    pub fn append(&mut self, epoch: u64, start_offset: u64) -> io::Result<()> {
        if let Some(&(last_epoch, last_start)) = self.entries.last() {
            if epoch == last_epoch && start_offset == last_start {
                return Ok(());
            }
            if epoch <= last_epoch || start_offset < last_start {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "epoch entry ({epoch}, {start_offset}) does not follow ({last_epoch}, {last_start})"
                    ),
                ));
            }
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut buf = [0u8; ENTRY_BYTES];
        buf[0..8].copy_from_slice(&epoch.to_le_bytes());
        buf[8..16].copy_from_slice(&start_offset.to_le_bytes());
        file.write_all(&buf)?;
        fsync_file(&file)?;
        self.entries.push((epoch, start_offset));
        Ok(())
    }

    /// Answers "where did `epoch` end in this history?" for a log ending at
    /// `log_end`: the start of the first later epoch, `log_end` if `epoch` is
    /// still current, or 0 if `epoch` predates the whole history (the asker
    /// shares nothing with us).
    pub fn end_offset_for(&self, epoch: u64, log_end: u64) -> u64 {
        let mut known = false;
        for &(entry_epoch, entry_start) in &self.entries {
            if entry_epoch > epoch {
                return if known { entry_start } else { 0 };
            }
            known = true;
        }
        if known { log_end } else { 0 }
    }

    /// Drops entries made obsolete by a log truncation to `new_next`
    /// (entries whose start offset is at or past the new end).
    pub fn truncate_to(&mut self, new_next: u64) -> io::Result<()> {
        let keep = self
            .entries
            .iter()
            .take_while(|&&(_, start)| start < new_next)
            .count();
        if keep == self.entries.len() {
            return Ok(());
        }
        self.entries.truncate(keep);
        let tmp = self.path.with_extension("tmp");
        {
            let mut file = File::create(&tmp)?;
            for &(epoch, start) in &self.entries {
                let mut buf = [0u8; ENTRY_BYTES];
                buf[0..8].copy_from_slice(&epoch.to_le_bytes());
                buf[8..16].copy_from_slice(&start.to_le_bytes());
                file.write_all(&buf)?;
            }
            fsync_file(&file)?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}
