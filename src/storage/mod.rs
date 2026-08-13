mod epoch;
mod index;
mod log;
mod record;
mod segment;

pub use epoch::EpochCheckpoint;
pub use log::{AppendInfo, Log};
pub use record::{DecodeOutcome, Record};

use std::fs::File;
use std::io;

/// When appended records are forced to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every append. Honest and slow; the durability floor.
    Always,
    /// Group commit: fsync when either `max_bytes` dirty bytes or `max_ms`
    /// milliseconds have accumulated since the last flush. Acks for these
    /// records must be held until the covering flush (the broker's job).
    Batch { max_bytes: u64, max_ms: u64 },
    /// Never fsync explicitly; the OS page cache decides. Kafka's per-broker
    /// default, because its durability story is replication, not the local disk.
    Os,
}

impl FsyncPolicy {
    /// Parses `always`, `os`, or `batch:<bytes>:<ms>` (the CLI/bench syntax).
    pub fn parse(s: &str) -> std::result::Result<Self, String> {
        match s {
            "always" => Ok(FsyncPolicy::Always),
            "os" => Ok(FsyncPolicy::Os),
            _ => {
                let rest = s
                    .strip_prefix("batch:")
                    .ok_or_else(|| format!("unknown fsync policy {s}"))?;
                let (bytes, ms) = rest
                    .split_once(':')
                    .ok_or_else(|| format!("batch policy must be batch:<bytes>:<ms>, got {s}"))?;
                Ok(FsyncPolicy::Batch {
                    max_bytes: bytes.parse().map_err(|e| format!("batch bytes: {e}"))?,
                    max_ms: ms.parse().map_err(|e| format!("batch ms: {e}"))?,
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub max_segment_bytes: u64,
    pub index_interval_bytes: u64,
    pub max_record_bytes: u32,
    pub fsync: FsyncPolicy,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 128 * 1024 * 1024,
            index_interval_bytes: 4096,
            max_record_bytes: 16 * 1024 * 1024,
            fsync: FsyncPolicy::Batch {
                max_bytes: 1024 * 1024,
                max_ms: 50,
            },
        }
    }
}

impl LogConfig {
    pub fn validate(&self) -> Result<()> {
        // Segment-relative file positions are stored as u32 in the index, so a
        // segment (plus one oversized record) must stay far below 4 GiB.
        if self.max_segment_bytes < 1024 || self.max_segment_bytes > (1 << 30) {
            return Err(StorageError::InvalidConfig(
                "max_segment_bytes must be within [1 KiB, 1 GiB]".into(),
            ));
        }
        if self.index_interval_bytes < 64 {
            return Err(StorageError::InvalidConfig(
                "index_interval_bytes must be at least 64".into(),
            ));
        }
        if self.max_record_bytes as u64 > (1 << 30) {
            return Err(StorageError::InvalidConfig(
                "max_record_bytes must be at most 1 GiB".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt data at file position {position} (near offset {offset}): {reason}")]
    Corrupt {
        offset: u64,
        position: u64,
        reason: String,
    },
    #[error("offset {0} out of range")]
    OffsetOutOfRange(u64),
    #[error("record too large: {0} bytes")]
    RecordTooLarge(u64),
    #[error("invalid log layout: {0}")]
    InvalidLayout(String),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// An fsync that actually reaches stable media. On macOS, fsync(2) only pushes
/// data to the drive's volatile write cache; F_FULLFSYNC is the real barrier.
#[cfg(target_os = "macos")]
pub(crate) fn fsync_file(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
    if rc == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn fsync_file(file: &File) -> io::Result<()> {
    file.sync_data()
}
