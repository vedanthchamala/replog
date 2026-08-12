use std::fs;
use std::path::{Path, PathBuf};

use replog::storage::{FsyncPolicy, Log, LogConfig, StorageError};

fn cfg(fsync: FsyncPolicy) -> LogConfig {
    LogConfig {
        fsync,
        ..LogConfig::default()
    }
}

fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "log"))
        .collect();
    v.sort();
    v
}

fn index_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "index"))
        .collect();
    v.sort();
    v
}

fn copy_dir(src: &Path, dst: &Path) {
    for e in fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        fs::copy(e.path(), dst.join(e.file_name())).unwrap();
    }
}

fn read_all(log: &Log) -> Vec<replog::storage::Record> {
    let mut out = Vec::new();
    let mut off = log.start_offset();
    while off < log.next_offset() {
        let recs = log.read(off, 1 << 20).unwrap();
        assert!(!recs.is_empty(), "read at in-range offset {off} returned nothing");
        for r in recs {
            assert_eq!(r.offset, off, "offsets must be dense and ordered");
            off += 1;
            out.push(r);
        }
    }
    out
}

#[test]
fn roundtrip_and_reopen_continuity() {
    let tmp = tempfile::tempdir().unwrap();
    let mut expected = Vec::new();
    {
        let mut log = Log::open(tmp.path(), cfg(FsyncPolicy::Os)).unwrap();
        for i in 0..1000u64 {
            let key = if i % 7 == 0 {
                None
            } else {
                Some(format!("key-{i}").into_bytes())
            };
            let value = match i % 5 {
                0 => Vec::new(),
                4 => vec![0xAB; 100 * 1024],
                _ => format!("value-{i}").into_bytes(),
            };
            let info = log.append(key.clone(), value.clone()).unwrap();
            assert_eq!(info.offset, i);
            expected.push((key, value));
        }
        log.flush().unwrap();
    }

    let mut log = Log::open(tmp.path(), cfg(FsyncPolicy::Os)).unwrap();
    assert_eq!(log.next_offset(), 1000);
    assert_eq!(log.durable_offset(), Some(999));
    let records = read_all(&log);
    assert_eq!(records.len(), 1000);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.key, expected[i].0);
        assert_eq!(r.value, expected[i].1);
    }

    let info = log.append(None, b"after-reopen".to_vec()).unwrap();
    assert_eq!(info.offset, 1000);

    assert!(log.read(0, 1024).is_ok());
    assert!(log.read(1001, 1024).unwrap().is_empty());
    assert!(matches!(
        log.read(1002, 1024),
        Err(StorageError::OffsetOutOfRange(_))
    ));
}

#[test]
fn segment_roll_and_reads_across_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let config = LogConfig {
        max_segment_bytes: 2048,
        index_interval_bytes: 256,
        fsync: FsyncPolicy::Os,
        ..LogConfig::default()
    };
    {
        let mut log = Log::open(tmp.path(), config.clone()).unwrap();
        for i in 0..500u64 {
            log.append(None, format!("v-{i}").into_bytes()).unwrap();
        }
        log.flush().unwrap();
        assert!(log.segment_count() > 3, "expected several segments");
        let records = read_all(&log);
        assert_eq!(records.len(), 500);
    }
    assert!(log_files(tmp.path()).len() > 3);

    let log = Log::open(tmp.path(), config).unwrap();
    let records = read_all(&log);
    assert_eq!(records.len(), 500);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.value, format!("v-{i}").into_bytes());
    }
}

/// The core crash-consistency property: truncate the log file at EVERY byte
/// position inside the final record, and recovery must always come back with
/// exactly the records whose bytes fully survived, then keep working.
#[test]
fn torn_tail_truncated_at_every_cut_point() {
    let tmp = tempfile::tempdir().unwrap();
    let config = cfg(FsyncPolicy::Os);
    let mut log = Log::open(tmp.path(), config.clone()).unwrap();
    for i in 0..4u64 {
        log.append(Some(vec![b'k'; 8]), format!("value-{i}").into_bytes())
            .unwrap();
    }
    log.flush().unwrap();
    let seg = log_files(tmp.path())[0].clone();
    let len_before_last = fs::metadata(&seg).unwrap().len();
    log.append(Some(vec![b'k'; 8]), b"final-record".to_vec())
        .unwrap();
    log.flush().unwrap();
    let len_full = fs::metadata(&seg).unwrap().len();
    drop(log);

    for cut in len_before_last..len_full {
        let case = tempfile::tempdir().unwrap();
        copy_dir(tmp.path(), case.path());
        let segf = log_files(case.path())[0].clone();
        let f = fs::OpenOptions::new().write(true).open(&segf).unwrap();
        f.set_len(cut).unwrap();
        drop(f);

        let mut log = Log::open(case.path(), config.clone())
            .unwrap_or_else(|e| panic!("recovery failed at cut {cut}: {e}"));
        assert_eq!(log.next_offset(), 4, "cut at {cut}");
        let info = log.append(None, b"after-recovery".to_vec()).unwrap();
        assert_eq!(info.offset, 4);
        let records = read_all(&log);
        assert_eq!(records.len(), 5);
        assert_eq!(records[4].value, b"after-recovery");
    }
}

#[test]
fn garbage_tail_is_truncated() {
    let tmp = tempfile::tempdir().unwrap();
    let config = cfg(FsyncPolicy::Os);
    {
        let mut log = Log::open(tmp.path(), config.clone()).unwrap();
        for i in 0..3u64 {
            log.append(None, format!("v-{i}").into_bytes()).unwrap();
        }
        log.flush().unwrap();
    }
    let seg = log_files(tmp.path())[0].clone();
    let mut bytes = fs::read(&seg).unwrap();
    bytes.extend_from_slice(&[0xFF; 64]);
    fs::write(&seg, &bytes).unwrap();

    let log = Log::open(tmp.path(), config).unwrap();
    assert_eq!(log.next_offset(), 3);
    assert_eq!(read_all(&log).len(), 3);
}

#[test]
fn corruption_in_rolled_segment_fails_loudly_on_open() {
    let tmp = tempfile::tempdir().unwrap();
    let config = LogConfig {
        max_segment_bytes: 1024,
        fsync: FsyncPolicy::Os,
        ..LogConfig::default()
    };
    {
        let mut log = Log::open(tmp.path(), config.clone()).unwrap();
        for i in 0..200u64 {
            log.append(None, format!("value-{i}").into_bytes()).unwrap();
        }
        log.flush().unwrap();
        assert!(log.segment_count() > 2);
    }
    let first = log_files(tmp.path())[0].clone();
    let mut bytes = fs::read(&first).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    fs::write(&first, &bytes).unwrap();

    let err = match Log::open(tmp.path(), config) {
        Ok(_) => panic!("open must fail on a corrupted rolled segment"),
        Err(e) => e,
    };
    assert!(
        matches!(err, StorageError::Corrupt { .. }),
        "expected loud corruption error, got: {err}"
    );
}

/// With a sparse index present, open-time recovery only scans a rolled
/// segment's tail — so mid-segment corruption must still be caught by the
/// read path's CRC check.
#[test]
fn corruption_detected_on_read() {
    let tmp = tempfile::tempdir().unwrap();
    let config = LogConfig {
        max_segment_bytes: 8192,
        index_interval_bytes: 128,
        fsync: FsyncPolicy::Os,
        ..LogConfig::default()
    };
    let mut log = Log::open(tmp.path(), config).unwrap();
    for i in 0..600u64 {
        log.append(None, format!("value-{i}").into_bytes()).unwrap();
    }
    log.flush().unwrap();
    assert!(log.segment_count() > 1);

    let first = log_files(tmp.path())[0].clone();
    let mut bytes = fs::read(&first).unwrap();
    bytes[100] ^= 0xFF;
    fs::write(&first, &bytes).unwrap();

    let mut saw_corrupt = false;
    let mut off = 0u64;
    while off < log.next_offset() {
        match log.read(off, 1 << 16) {
            Ok(recs) if recs.is_empty() => break,
            Ok(recs) => off = recs.last().unwrap().offset + 1,
            Err(StorageError::Corrupt { .. }) => {
                saw_corrupt = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert!(saw_corrupt, "corrupted record served without error");
}

#[test]
fn index_files_rebuilt_when_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let config = LogConfig {
        max_segment_bytes: 4096,
        index_interval_bytes: 128,
        fsync: FsyncPolicy::Os,
        ..LogConfig::default()
    };
    {
        let mut log = Log::open(tmp.path(), config.clone()).unwrap();
        for i in 0..300u64 {
            log.append(Some(format!("k{i}").into_bytes()), format!("value-{i}").into_bytes())
                .unwrap();
        }
        log.flush().unwrap();
    }
    for idx in index_files(tmp.path()) {
        fs::remove_file(idx).unwrap();
    }

    let log = Log::open(tmp.path(), config).unwrap();
    let records = read_all(&log);
    assert_eq!(records.len(), 300);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.value, format!("value-{i}").into_bytes());
    }
    assert!(
        !index_files(tmp.path()).is_empty(),
        "index files should be recreated on recovery"
    );
}

#[test]
fn durability_tracking_follows_policy() {
    // Os: nothing is durable until an explicit flush.
    let tmp = tempfile::tempdir().unwrap();
    let mut log = Log::open(tmp.path(), cfg(FsyncPolicy::Os)).unwrap();
    let info = log.append(None, b"a".to_vec()).unwrap();
    assert_eq!(info.durable_offset, None);
    assert_eq!(log.flush().unwrap(), Some(0));

    // Always: every append is durable by the time it returns.
    let tmp = tempfile::tempdir().unwrap();
    let mut log = Log::open(tmp.path(), cfg(FsyncPolicy::Always)).unwrap();
    for i in 0..5u64 {
        let info = log.append(None, vec![b'x'; 8]).unwrap();
        assert_eq!(info.durable_offset, Some(i));
    }

    // Batch by bytes: durability advances only once the dirty budget trips.
    let tmp = tempfile::tempdir().unwrap();
    let mut log = Log::open(
        tmp.path(),
        cfg(FsyncPolicy::Batch {
            max_bytes: 64,
            max_ms: 3_600_000,
        }),
    )
    .unwrap();
    let a = log.append(None, vec![b'y'; 16]).unwrap();
    assert_eq!(a.durable_offset, None, "48 dirty bytes should not trip a 64-byte budget");
    let b = log.append(None, vec![b'y'; 16]).unwrap();
    assert_eq!(b.durable_offset, Some(1), "96 dirty bytes must trip the budget");
}
