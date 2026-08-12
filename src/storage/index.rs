use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

pub const ENTRY_BYTES: usize = 8;

/// Sparse offset index for one segment: `(relative_offset, file_position)`
/// pairs, one per ~index_interval_bytes of log written.
///
/// The index is derived state — a pure cache over the log. If it is missing or
/// malformed it is rebuilt by scanning the segment, so it is never fsynced and
/// can never become a source of truth to corrupt.
pub struct SparseIndex {
    entries: Vec<(u32, u32)>,
    file: File,
}

impl SparseIndex {
    pub fn create(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            entries: Vec::new(),
            file,
        })
    }

    /// Loads an existing index. Returns `Ok(None)` if the file is absent or
    /// malformed — the caller rebuilds from the log instead of failing.
    pub fn load(path: &Path) -> std::io::Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        if bytes.len() % ENTRY_BYTES != 0 {
            return Ok(None);
        }
        let mut entries: Vec<(u32, u32)> = Vec::with_capacity(bytes.len() / ENTRY_BYTES);
        for chunk in bytes.chunks_exact(ENTRY_BYTES) {
            let rel = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
            let pos = u32::from_le_bytes(chunk[4..8].try_into().unwrap());
            if let Some(&(last_rel, last_pos)) = entries.last()
                && (rel <= last_rel || pos <= last_pos) {
                    return Ok(None);
                }
            entries.push((rel, pos));
        }
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Some(Self { entries, file }))
    }

    pub fn append(&mut self, relative_offset: u32, file_position: u32) -> std::io::Result<()> {
        let mut buf = [0u8; ENTRY_BYTES];
        buf[0..4].copy_from_slice(&relative_offset.to_le_bytes());
        buf[4..8].copy_from_slice(&file_position.to_le_bytes());
        self.file.write_all(&buf)?;
        self.entries.push((relative_offset, file_position));
        Ok(())
    }

    /// Greatest entry with `relative_offset <= target`, if any.
    pub fn lookup(&self, target_relative_offset: u32) -> Option<(u32, u32)> {
        match self
            .entries
            .binary_search_by_key(&target_relative_offset, |&(rel, _)| rel)
        {
            Ok(i) => Some(self.entries[i]),
            Err(0) => None,
            Err(i) => Some(self.entries[i - 1]),
        }
    }

    pub fn last(&self) -> Option<(u32, u32)> {
        self.entries.last().copied()
    }
}
