//! Stage 4 storage additions: suffix truncation (the follower-reconciliation
//! primitive), replicated appends, and the leader-epoch checkpoint.

use replog::storage::{EpochCheckpoint, FsyncPolicy, Log, LogConfig, Record};

fn tiny_config() -> LogConfig {
    LogConfig {
        max_segment_bytes: 1024,
        index_interval_bytes: 128,
        fsync: FsyncPolicy::Os,
        ..LogConfig::default()
    }
}

fn build_log(dir: &std::path::Path, records: u64) -> Log {
    let mut log = Log::open(dir, tiny_config()).unwrap();
    for i in 0..records {
        log.append(Some(format!("k{i}").into_bytes()), format!("value-{i}").into_bytes())
            .unwrap();
    }
    log.flush().unwrap();
    log
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

#[test]
fn truncate_suffix_at_every_offset() {
    let source = tempfile::tempdir().unwrap();
    let total = 120u64;
    let log = build_log(source.path(), total);
    assert!(log.segment_count() > 3, "test needs multiple segments");
    drop(log);

    let work_root = tempfile::tempdir().unwrap();
    for cut in 0..=total {
        let dir = work_root.path().join(format!("cut-{cut}"));
        copy_dir(source.path(), &dir);
        let mut log = Log::open(&dir, tiny_config()).unwrap();
        log.truncate_suffix(cut).unwrap();
        assert_eq!(log.next_offset(), cut, "cut at {cut}");

        let mut offset = 0u64;
        while offset < cut {
            let records = log.read(offset, 1 << 20).unwrap();
            assert!(!records.is_empty());
            for r in &records {
                assert_eq!(r.offset, offset);
                assert_eq!(r.value, format!("value-{offset}").into_bytes());
                offset += 1;
            }
        }
        assert!(log.read(cut, 1 << 20).unwrap().is_empty());

        // The log keeps working after truncation, and survives reopen.
        let info = log.append(None, b"after-truncate".to_vec()).unwrap();
        assert_eq!(info.offset, cut);
        log.flush().unwrap();
        drop(log);
        let log = Log::open(&dir, tiny_config()).unwrap();
        assert_eq!(log.next_offset(), cut + 1);
        let last = log.read(cut, 1 << 20).unwrap();
        assert_eq!(last[0].value, b"after-truncate".to_vec());
    }
}

#[test]
fn replicated_appends_preserve_offsets_and_reject_gaps() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = build_log(leader_dir.path(), 50);
    let mut follower = Log::open(follower_dir.path(), tiny_config()).unwrap();

    let mut fetched = Vec::new();
    let mut pos = 0;
    while pos < leader.next_offset() {
        let batch = leader.read(pos, 4096).unwrap();
        pos = batch.last().unwrap().offset + 1;
        fetched.extend(batch);
    }
    for record in &fetched {
        follower.append_replicated(record.clone()).unwrap();
    }
    assert_eq!(follower.next_offset(), leader.next_offset());
    let replica_view = follower.read(10, 1 << 20).unwrap();
    let leader_view = leader.read(10, 1 << 20).unwrap();
    assert_eq!(replica_view, leader_view, "timestamps and keys must match too");

    let gap = Record {
        offset: follower.next_offset() + 5,
        timestamp_ms: 0,
        key: None,
        value: b"gap".to_vec(),
    };
    assert!(follower.append_replicated(gap).is_err());

    // Leaders keep assigning offsets correctly after replicated appends.
    let info = leader.append(None, b"tail".to_vec()).unwrap();
    assert_eq!(info.offset, 50);
}

#[test]
fn epoch_checkpoint_roundtrip_and_queries() {
    let dir = tempfile::tempdir().unwrap();
    let mut ck = EpochCheckpoint::load(dir.path()).unwrap();
    assert_eq!(ck.current_epoch(), 0);
    assert_eq!(ck.end_offset_for(3, 100), 0, "unknown epoch shares nothing");

    ck.append(1, 0).unwrap();
    ck.append(3, 40).unwrap();
    ck.append(4, 90).unwrap();
    assert!(ck.append(2, 95).is_err(), "epochs must advance");

    let reloaded = EpochCheckpoint::load(dir.path()).unwrap();
    assert_eq!(reloaded.entries(), &[(1, 0), (3, 40), (4, 90)]);
    assert_eq!(reloaded.current_epoch(), 4);

    // Epoch 1 ended where epoch 3 began. Epoch 2 is unknown here, so the
    // answer falls back to the end of the largest known epoch <= 2 (epoch 1).
    assert_eq!(reloaded.end_offset_for(1, 120), 40);
    assert_eq!(reloaded.end_offset_for(2, 120), 40);
    assert_eq!(reloaded.end_offset_for(3, 120), 90);
    assert_eq!(reloaded.end_offset_for(4, 120), 120, "current epoch runs to log end");
    assert_eq!(reloaded.end_offset_for(9, 120), 120);
    assert_eq!(reloaded.end_offset_for(0, 120), 0, "predates history entirely");

    let mut ck = reloaded;
    ck.truncate_to(60).unwrap();
    assert_eq!(ck.entries(), &[(1, 0), (3, 40)]);
    let reloaded = EpochCheckpoint::load(dir.path()).unwrap();
    assert_eq!(reloaded.entries(), &[(1, 0), (3, 40)]);
}
