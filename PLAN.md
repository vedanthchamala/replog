# PLAN — current stage in detail, later stages as skeleton

Rule (borrowed from previous projects): the *next* stage is planned in detail; later
stages stay skeletal until their predecessor's demo passes, because early decisions
shouldn't over-constrain what the working system teaches us.

---

## Stage 1 (ACTIVE) — single-node storage engine

The disk layer everything else sits on. No network, no threads beyond tests. The whole
stage is synchronous Rust: a `Log` you can `append()` to and `read()` from that survives
being killed at any instant.

### On-disk format (little-endian throughout)

A partition is a directory. Inside it, segments:

```
00000000000000000000.log      records, append-only
00000000000000000000.index    sparse index for that segment
00000000000000042817.log      next segment (base offset = first record's offset)
00000000000000042817.index
```

Record layout in a `.log` file:

```
u32  len          length of everything after this field
u32  crc32        over everything after this field (IEEE polynomial, crc32fast)
u64  offset       absolute, monotonic
i64  timestamp_ms
i32  key_len      -1 = null key
     key bytes
u32  value_len
     value bytes
```

Why length-first: recovery can skip records without parsing them. Why CRC after len:
the CRC protects offset through value, so a torn or bit-flipped record is detected
before anything is trusted. Why absolute offset *in* the record: self-describing files;
recovery and the checker can validate continuity without external state.

Index entry layout in an `.index` file (8 bytes each):

```
u32  relative_offset   (record offset - segment base offset)
u32  file_position     byte position of that record in the .log
```

Sparse: one entry per INDEX_INTERVAL_BYTES (default 4 KiB) of log written. Lookup =
binary search for greatest entry ≤ target, then sequential scan. The index is a pure
cache: if missing or corrupt it is rebuilt by scanning the segment.

### Fsync policy (the measured decision)

```rust
enum FsyncPolicy {
    Always,                                  // fsync after every append
    Batch { max_bytes: u64, max_ms: u64 },   // group commit: fsync when either trips
    Os,                                      // never fsync explicitly; OS page cache decides
}
```

`Log::append()` returns the offset AND whether the record is durable yet;
`Log::flush()` forces it. The broker (Stage 2) will hold acks until the covering flush
completes — that is what makes group commit safe rather than a lie. Stage 1 benches all
three policies so the durability/latency trade-off is a plotted curve, not folklore.

### Recovery algorithm (on `Log::open`)

1. List `*.log`, sort by base offset. Each segment's base must equal the previous
   segment's end; gaps = hard error (refuse to open, corruption is not guessed around).
2. Rolled segments: scan from the last index entry to EOF (cheap — at most
   ~index_interval bytes of records) to establish the end offset and validate the
   tail; full scan + index rebuild only if the index is missing or malformed. Any
   invalid record here is a hard error, never a truncation (rolled segments were
   fsynced at roll time; a bad record is corruption, not a torn write).
3. Last segment: full sequential scan with the index always rebuilt (recovery may
   truncate the log, and a derived index must never outlive the bytes it maps).
   First record that fails length/CRC/continuity marks the torn tail → truncate.
4. `next_offset` = last valid record + 1. Append continues in the same segment.

Middle-of-file corruption (CRC fails on a *rolled* segment during read) is surfaced as
an error, never silently truncated — truncation is only ever a tail operation.

### Module plan

```
src/storage/record.rs    Record struct, encode/decode, DecodeOutcome {Ok, Incomplete, Corrupt}
src/storage/index.rs     SparseIndex: append entry, lookup, load, rebuild
src/storage/segment.rs   Segment: append, read_from(offset), scan/recover, roll decision
src/storage/log.rs       Log: open/recover, append, read, flush, segment management
src/storage/mod.rs       FsyncPolicy, LogConfig, error types
```

### Tests (pass conditions in executable form)

1. Round-trip: 1k records (incl. null keys, empty values, 100 KiB values) read back
   byte-identical.
2. Reopen continuity: append, drop, reopen → offsets continue, all records readable.
3. Segment roll: tiny `max_segment_bytes` forces many segments; reads cross boundaries.
4. Torn tail: truncate the active `.log` at every byte position inside the final record
   (property-style loop) → reopen always succeeds, always yields exactly the records
   whose full bytes survived, next append continues cleanly.
5. Injected corruption: flip one byte in a value mid-file → read reports corruption,
   recovery does NOT silently truncate a rolled segment.
6. Index rebuild: delete the `.index` files → open rebuilds them, lookups still correct.
7. Sparse lookup correctness: random offsets across a multi-segment log.

### Bench (end of stage)

`bench/append_bench.rs` (or a bin): N appends of M-byte records under each fsync
policy; report appends/sec, MB/s, p50/p99 append latency. CSV out; one plot. This is
the stage's headline measurement: what an fsync actually costs on this SSD, and what
group commit buys back.

---

## Stage 2 (skeleton) — TCP broker + clients

- tokio; length-prefixed frames (u32 len + u16 msg type + body); hand-rolled encoding
  shared by client and broker (one `proto` module, round-trip property tests).
- Requests: Produce (topic, partition, records, acks), Fetch (topic, partition, offset,
  max_bytes, max_wait — long-poll), CommitOffset / FetchOffset (group, topic, partition),
  Metadata. Correlation IDs for pipelining.
- Broker: one async task per connection; per-partition writer task owning the `Log`
  (single-writer principle — same reasoning as nanoserve's engine thread); ack held
  until covering flush under Batch policy.
- Consumer offsets stored in an internal `__offsets` log (dogfooding the log as its own
  metadata store, like Kafka).
- Bench: throughput vs producer batch size; latency percentiles at fixed arrival rate.

## Stage 3 (skeleton) — partitions + consumer groups

- Topics with N partitions; producer partitioner = hash(key) % N, round-robin for null.
- Group coordinator on the broker: join/leave/heartbeat, generation numbers, range
  assignment, rebalance on membership change; fencing by generation on offset commits.
- Checker v1: at-least-once delivery of every produced record across a rebalance.

## Stage 4 (skeleton) — replication + failover

- Controller (single, static for now) holds cluster metadata; brokers heartbeat it.
- Followers replicate via the fetch path; leader tracks ISR by follower lag/liveness;
  high-water mark = min(ISR match offsets); consumers read only up to HWM.
- acks=0/1/all semantics at the produce path; `min.insync.replicas` equivalent.
- Leader epochs: every leadership change bumps the epoch; fetches carry epoch; a
  rejoining stale leader truncates its divergent suffix via epoch/offset check.
- Failover demo: 3 brokers, kill -9 the leader under load, measure detection→election→
  first-ack gap, checker verifies zero acked-loss.

## Stage 5 (skeleton) — torture harness

- Seeded fault scheduler: process kill/restart, network partition (proxy layer),
  slow disk (optional). Client-side history logging (every produce attempt + ack,
  every consumed record).
- Offline checker: acked ⇒ eventually consumed (durability); per-partition offset
  monotonicity, gap-free up to HWM; duplicate accounting (at-least-once budget).
- Run matrix in CI-able script; every violation found becomes a LOG.md war story.

## Stage 6 (stretch, pick by what the measurements say)

Idempotent producer (producer id + sequence dedup) → exactly-once; compaction;
sendfile/`copy_file_range` zero-copy fetch path; 3× GCP e2 deployment with
cross-zone latency measured.
