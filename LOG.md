# Build log

Dated, append-only. One entry per working session: what was done, what was decided and
why, what broke, what's next. War stories land here first.

---

## 2026-08-12 — project kickoff

**Done:** Chose the project after a portfolio gap analysis: existing work covers GPU
kernels, distributed training collectives (ring-allreduce), and LLM serving
(nanoserve), but nothing covers replication, durability, consensus-adjacent
coordination, or surviving machine failure — the core of distributed *data* systems.
A Kafka-style replicated log hits all of it, and the from-scratch + measure-everything
method carries over from the previous two systems projects.

**Decisions made today (rationale in interview/NOTES.md, condensed here):**

- **Rust** over Go/C++: new language depth + ownership/borrow discipline maps well to
  a storage engine (buffers, lifetimes of mmap'd/owned regions); tokio for the broker
  later. Cost accepted: slower first stage while learning idioms.
- **Own wire protocol, not Kafka's:** Kafka protocol compatibility is a serialization
  grind that doesn't teach distributed systems; a hand-rolled length-prefixed protocol
  does (framing, versioning, correlation IDs).
- **ISR replication (Kafka-style), not Raft, for the data path:** f+1 replicas tolerate
  f failures under ISR vs 2f+1 under quorum — the actual reason Kafka's data plane
  works the way it does. Controller plane kept to a single-controller simplification.
- **Fsync policy as a first-class, benchmarked config** (`always` / group-commit /
  `os`) rather than a hardcoded choice: the durability-vs-latency curve is Stage 1's
  headline measurement.
- **Little-endian on disk and wire** (document, don't inherit network byte order by
  reflex — every target CPU is LE; the format is explicit either way).
- **Recovery truncates only the tail of the last segment.** Mid-file corruption in a
  rolled segment is a surfaced error, never a silent truncation — silently dropping
  acked data to "recover" would violate the durability contract from the inside.

**Next:** Stage 1 modules + tests per PLAN.md; append benchmark; first LOG entry with
real numbers.

---

## 2026-08-12 (later) — Stage 1 storage engine: core landed, tests green

**Done:** `record` / `index` / `segment` / `log` modules implemented per PLAN.md.
8 integration tests pass, including the key one: truncate the log file at *every*
byte position inside the final record (property-style loop over ~50 cut points) —
recovery always returns exactly the records whose bytes fully survived, and the log
keeps appending cleanly afterward. Also covered: reopen continuity, multi-segment
rolls, garbage-tail truncation, loud failure on rolled-segment corruption (both at
open and at read), index deletion → rebuild, and durable-offset tracking under all
three fsync policies.

**Decisions during implementation:**

- **macOS fsync is not durable** — `fsync(2)` stops at the drive's volatile cache;
  `fcntl(F_FULLFSYNC)` is the real media barrier. `fsync_file()` abstracts this
  per-platform. This will make the fsync-policy benchmark curve dramatic.
- **Roll fsyncs the outgoing segment first.** Recovery trusts rolled segments (no
  torn-tail truncation there), and that trust is only sound because the roll makes
  them durable before the new segment exists.
- **The active segment's index is always rebuilt at recovery** — truncation could
  leave index entries pointing past the new EOF, and a derived index must never
  outlive the bytes it maps. Rolled segments reuse their index and scan only from
  its last entry, keeping recovery O(tail) rather than O(log).
- CRC is crc32 (IEEE, `crc32fast`), not crc32c as first drafted in PLAN.md —
  hardware-accelerated either way; PLAN corrected to match the code.

**Next:** `bench/` append benchmark (fsync=always vs batch vs os → CSV + plot),
then Stage 2 planning (tokio broker + wire protocol).

---

## 2026-08-13 — Stage 1 COMPLETE: the fsync-policy curve, measured

**Done:** `src/bin/append_bench.rs` + `bench/run_append_bench.sh` (policy × value-size
matrix → CSV) + `bench/plot_append.py` (uv/matplotlib → PNG). Results committed in
`bench/results/`. Methodology note: every policy is timed to the same finish line —
a final flush is included — so `os`/`batch` get no credit for bytes still in the
page cache.

**Numbers (MacBook M4 Pro, APFS SSD, single-threaded, `bench/run_append_bench.sh`):**

| policy | 100 B values | 1 KiB values |
|---|---|---|
| always (F_FULLFSYNC per append) | 238 appends/s, p50 4.05 ms | 238 appends/s, p50 4.03 ms |
| batch 1 MiB / 50 ms | 550k appends/s, p50 0.9 µs | 110k appends/s, p50 3.0 µs |
| os | 955k appends/s, p50 0.9 µs | 521k appends/s, p50 1.2 µs |

**What the curve says:** one true media barrier costs ~4.2 ms on this SSD, so
fsync-per-append caps the engine at ~238 appends/sec regardless of record size — the
disk is the clock. Group commit buys back ~2,300× at 100 B by amortizing one barrier
over ~1 MiB of appends; the p999 under batch (~3.5 ms) is the one record that trips
the flush and pays for everyone. `os` is memcpy-into-page-cache speed — quotable only
with the caveat that durability is deferred to the OS (or, later, to replication,
which is Kafka's actual answer).

**Stage 1 exit review vs SPEC pass conditions:** torn-write recovery test (every-byte
cut-point loop) ✅ · CRC catches injected corruption, rolled-segment corruption is a
loud error ✅ · fsync-policy bench plotted ✅. Stage 1 closed; 8/8 tests green.

**Next:** Detail Stage 2 in PLAN.md (wire protocol + tokio broker), build it,
first PDF compile at the milestone.

---

## 2026-08-13 (later) — Stage 2 COMPLETE: TCP broker + clients, measured

**Built:** `proto` (length-prefixed LE frames, correlation IDs, round-trip +
truncation/garbage rejection tests), `broker` (one writer *thread* per partition
owning its Log; durable acks parked in a ledger until the covering flush; long-poll
fetch via a per-partition `watch` of next_offset; `__offsets` internal log replayed
at startup), `client` (correlation-routed pipelined connection, batching Producer,
committing/resuming Consumer), `replog_broker` bin. 18 tests green (4 proto,
6 broker, 8 storage).

**The test that matters:** broker as a real child process, produce at acks=durable,
`kill -9`, restart on the same data dir → all 50 acked records fetchable. The
Stage 2 form of the SPEC durability contract, checked at the process boundary.

**War story — the double-fsync (full writeup in interview/NOTES.md §3):** first
bench run showed durable p50 ~2× expected, written throughput ~half of storage
speed, and a *non-monotonic* pipelining sweep. One root cause: the Log's internal
group-commit timer and the broker's timer both fired — first append after a quiet
gap paid a stray mid-batch F_FULLFSYNC. Fix: the partition thread owns time-based
flushing exclusively (Log keeps only the byte trigger). written b100 279k → 517k
rec/s; durable b1000 40k → 56k; sweep became monotonic. Lesson: two owners of one
timer = a quiet cost doubling only a benchmark can see.

**Numbers (localhost, 100 B values, one partition, fsync batch:1MiB:5ms;
`bench/run_broker_bench.sh`):** sync batch curve written 21.8k → 553k rec/s
(b1→b1000), durable 91 → 56k rec/s (one flush covers the batch; p50 stays ~11–18 ms
— the group-commit window + barrier). Pipelining (b100): 526k → 646k rec/s,
saturating on the single partition writer — by design; partitions are the scaling
knob (Stage 3). Open-loop rows (no coordinated omission): durable@20k/s p99 = 85 ms
vs closed-loop 27 ms — F_FULLFSYNC variance stacks into the tail under sustained
load.

**Stage 2 exit review vs SPEC:** end-to-end produce→fetch ✅ (byte-identical 5k
round trip) · consumer resumes from committed offset after restart ✅ (broker
restarted too; `__offsets` replay verified) · bench throughput vs batch size +
latency percentiles at fixed rate ✅ (open-loop mode). Stage 2 closed.

**Next:** interview PDF first compile (Stage 2 milestone), then Stage 3 planning
(partitions + consumer groups + rebalance + checker v1).

---

## 2026-08-13 (later) — interview PDF milestone + Stage 3 COMPLETE

**PDF:** first pdflatex compile of `interview/replog_guide.tex` (7 pages) from
NOTES.md material — pitch, decision log, war stories, numbers, glossary, Q&A,
honesty box. (Private, git-ignored, as always.)

**Built (Stage 3):** JoinGroup/Heartbeat/LeaveGroup protocol + generation-fenced
CommitOffset; broker-side group coordinator (range assignment, generation bump
per membership change, 200 ms eviction sweep); `TopicProducer` (crc32 key-hash
routing, per-partition batches); `GroupConsumer` (poll-driven heartbeats,
auto-rejoin, fenced commits); **checker v1** as a library — at-least-once,
duplicate accounting, per-generation offset monotonicity, same-offset⇒same-id.
28 tests green.

**Checker taught us the contract:** first draft flagged post-rebalance rewind to
the committed offset as a monotonicity violation — but that rewind IS
at-least-once redelivery. Rule corrected to "monotonic within one assignment
generation." Writing checkers forces the contract to get precise.

**The stage eval** (SPEC pass condition): 4 partitions, 3000 keyed records,
2 group consumers, consumer B crashes (no Leave) mid-stream → evicted at session
timeout → A absorbs all partitions. Checker: 3000/3000 acked ids consumed,
**zero at-least-once violations**, 400 duplicates counted honestly, takeover
1.16 s after crash (decomposes exactly into 1 s session timeout + sweep +
heartbeat interval). `cargo test --test group_tests rebalance_preserves`.

**Measured (partition fan-out, one broker):** three findings that matter:
(1) durable + pipelining ≈ written — 57k → 590k rec/s at inflight 8, one
partition: eight batches share each barrier, so the durable/written gap is the
price of *waiting alone*, not of durability. (2) More partitions made durable
throughput WORSE on one disk (590k → 211k at 8 partitions): per-partition
writers fragment group commit into N serialized device barriers. (3) The
~600k rec/s written ceiling is the *machine*, not a component — proven by
conn-per-partition (no change) and two concurrent client processes (they split
the same total, 273k+273k). Partition scaling is a cross-machine story;
measured properly at Stage 4.

**Stage 3 exit review vs SPEC:** 2 consumers split N partitions ✅ · kill one →
rebalance ✅ (session-timeout eviction, takeover measured) · every record
delivered ≥ once, verified by checker ✅. Stage 3 closed.

**Next:** Stage 4 planning (replication: controller, follower fetch, ISR,
acks=all, leader epochs, failover) — the project's centerpiece.
