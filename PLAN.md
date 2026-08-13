# PLAN — current stage in detail, later stages as skeleton

Rule (borrowed from previous projects): the *next* stage is planned in detail; later
stages stay skeletal until their predecessor's demo passes, because early decisions
shouldn't over-constrain what the working system teaches us.

---

## Stage 1 (DONE 2026-08-13) — single-node storage engine

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

## Stage 2 (ACTIVE) — TCP broker + clients

tokio broker + hand-rolled binary protocol + producer/consumer clients. The stage's
point: the ack contract (when is a produce "accepted"?) becomes real — under group
commit, acks are *held until the covering flush*, which is what makes Batch a safe
policy rather than a lie. Single broker, no replication yet; topics have partitions
but consumer *groups* wait for Stage 3.

### Wire protocol (`src/proto/`)

Frame (little-endian, both directions):

```
u32  len         length of everything after this field
u8   version     protocol version (1)
u8   msg_type
u32  correlation_id
     body        per message type
```

Correlation IDs are chosen by the client and echoed by the broker; responses may
arrive out of order (long-poll fetches), which is what makes pipelining safe.
Primitives: strings = u16 len + UTF-8; bytes = u32 len; optional bytes = i32 len
with -1 = null (same convention as the record format). Max frame 32 MiB.

Requests / responses (msg_type = request, response = request | 0x80):

| type | request body | response body |
|---|---|---|
| 1 CreateTopic | topic, u32 partitions | u16 error |
| 2 Metadata | — | u16 error, topics: [(topic, u32 partitions)] |
| 3 Produce | topic, u32 partition, u8 acks, records: [(key?, value)] | u16 error, u64 base_offset, u32 count (no response at all for acks=0) |
| 4 Fetch | topic, u32 partition, u64 offset, u32 max_bytes, u32 max_wait_ms | u16 error, u64 log_start, u64 next_offset, records: [(u64 offset, i64 ts, key?, value)] |
| 5 CommitOffset | group, topic, u32 partition, u64 offset | u16 error |
| 6 FetchOffset | group, topic, u32 partition | u16 error, i64 offset (-1 = none) |

`acks`: 0 = fire-and-forget (no response frame, Kafka-style), 1 = written (in the
log, page cache), 2 = durable (covering flush completed). With one broker, 2 is the
strongest contract available; at Stage 4 “all” layers replication on top of it.

Error codes (u16): 0 ok, 1 unknown topic/partition, 2 offset out of range,
3 storage error, 4 malformed request, 5 topic exists.

### Broker (`src/broker/`)

- `src/bin/replog_broker.rs`: `--listen addr --data-dir path --fsync always|os|batch:<bytes>:<ms>`.
- One tokio task per connection: decode frame → dispatch → write response frames
  (writes serialized through a per-connection mpsc so pipelined responses interleave
  cleanly).
- **One writer task per partition owning its `Log`** (single-writer principle).
  Commands arrive on an mpsc channel; appends reply via oneshot.
- **Ack ledger:** for acks=durable under Batch, the partition task parks the reply
  in a pending list keyed by last offset; every flush drains entries ≤ durable_offset.
  A tokio timer fires the time-based flush (`max_ms`) so a lone record's ack is never
  stranded (Stage 1's Log only flushes inside append calls; the broker owns time).
- **Long-poll fetch:** each partition task publishes `next_offset` on a
  `tokio::sync::watch`; a fetch at the log end subscribes and waits (bounded by
  `max_wait_ms`) instead of busy-polling.
- Reads go through the partition task too (actor model) — serializing reads with
  writes per partition is the simple correct baseline; measure before optimizing.
- **Offsets store:** internal `__offsets` topic (1 partition), dogfooding the log:
  commit = append of (group|topic|partition → offset) record; broker replays it into
  a HashMap at startup. Compaction is deliberately absent (Stage 6 stretch).
- Topics = directories `<data-dir>/<topic>-<partition>/`; partition count inferred
  at startup, CreateTopic makes the dirs. (Controller owns metadata at Stage 4.)

### Clients (`src/client/`)

- `Connection`: framed TCP + correlation-id routing table (background read task →
  oneshot per in-flight request) — pipelining for free.
- `Producer`: buffers (key, value) pairs; flushes as one Produce when `batch_records`
  is reached or on explicit `flush()`; configurable `acks`. Returns base offsets.
- `Consumer`: `poll()` fetch loop tracking position; `commit()`/resume via
  CommitOffset/FetchOffset with a group name (name only — real group membership is
  Stage 3).

### Tests (pass conditions in executable form)

1. proto round-trips: every message type encode→decode identity, plus truncated-buffer
   and garbage-header rejection (unit tests in `proto`).
2. e2e in-process: create topic → produce 10k mixed records (null keys, big values)
   in batches → fetch all → byte-identical, offsets contiguous.
3. Long-poll: fetch at log end parks; a produce during the wait completes it early
   with the new records.
4. **Crash durability across the ack boundary:** broker as a child *process*
   (`CARGO_BIN_EXE_replog_broker`), produce with acks=durable, `kill -9`, restart on
   the same data dir → every acked record is fetchable. (The Stage-2 version of the
   SPEC's durability contract.)
5. Consumer resume: produce 100, consume 60, commit, restart broker, new consumer
   with the same group resumes at 60.
6. Pipelining: N produces sent before any response is awaited; all responses arrive
   and match their correlation IDs.

### Bench (end of stage)

`src/bin/broker_bench.rs` + `bench/run_broker_bench.sh` → CSV + plot:
- Throughput vs producer batch size (1/10/100/1000 records per Produce) at
  acks=written and acks=durable (batch fsync), 100 B values, localhost.
- Produce round-trip latency percentiles (p50/p99) per batch size.
Headline: what client-side batching buys over TCP, and what the durable-ack
contract costs vs written-ack.

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
