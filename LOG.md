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

---

## 2026-08-13 (later) — Stage 4 core lands: replication evals green after a three-bug hunt

**Where the last session left off:** Stage 4's build was committed (storage
primitives, cluster protocol, controller, broker cluster mode) but the working
tree held the unfinished half: `ClusterClient` and the four replication evals —
and the evals didn't pass. Two hung *forever*, two failed. A leftover test
process from the interrupted session was still running, mid-hang; sampling it
(`/usr/bin/sample`, thread stacks) was the starting evidence.

**Bug 1 — the parked waiter (the hangs).** Client `Connection`'s read task, on
socket EOF, failed all in-flight calls and exited — but a call started *after*
that leaves a correlation-id waiter nothing will ever complete (the write into a
FIN'd loopback socket succeeds silently). The replica fetcher hit it
deterministically at leader shutdown: last long-poll completes, socket closes,
next call parks forever → fetcher pins its `Arc<Replica>` → partition writer
thread never sees channel disconnect → broker shutdown joins forever. Same hole
on the client side could park the failover producer mid-attempt (its retry
deadline was only checked between attempts). Fix: a `closed` flag on the pending
map, set by the read task on exit *under the same lock* `call_start` inserts
with — after connection death every new call fails fast with Closed. Static
ownership analysis kept proving the hang impossible; per-await instrumentation
found it in one run. Full writeup in interview/NOTES.md §3.

**Bug 2 — the fetch that outlived its kill.** The stale-leader eval kills a
follower whose fetcher had a long-poll in flight; the response (carrying one of
the old leader's acks=written records) was appended by the "dead" fetcher before
its next fencing check, so the restarted follower carried 101 records, recorded
epoch 2 at 101, and the epoch protocol faithfully converged both logs around a
record that was never committed history. Production shape of the same bug: a
follower promoted to leader with a fetch in flight appends stale records into
the new epoch's log. Fix: re-check the fetcher generation after *every* await,
immediately before append/truncate. Fencing is per-await, not per-loop.

**Bug 3 — ISR membership by past glory.** The keep rule `(in_isr && fresh) ||
caught_up` let a dead-but-once-caught-up follower stay in the ISR forever (its
frozen match offset equals a log end that nothing moves), so killing both
followers never shrank the ISR and acks=all was still accepted. Fix: Kafka's
lastCaughtUpTimeMs move — the leader timestamps each follower's last caught-up
fetch, and one rule serves shrink and expand: in the ISR iff caught up within
the lag window (never-fetched followers measured from leader takeover).

Plus a test-harness bug for the collection: `Cluster::shutdown(self)` dropped
the `TempDir`, deleting the data directories the test then "read back" as empty
— the eval erased its own evidence. Shutdown now returns the tempdir.

**Evals now green (the SPEC pass condition, in-process form):** kill the leader
mid-stream at acks=all → new leader, **1200/1200 acked ids survive, zero lost**,
first post-kill ack ~1.8 s (dominated by the 700 ms liveness timeout); replicas
converge byte-identical; ISR shrinks on death and acks=all is *refused* below
min-ISR; stale leader truncates its divergent suffix via epoch check and
rejoins. `cargo test`: **35 tests green** across the workspace.

**Next:** Stage 4 close-out per PLAN — the acks=0/1/all replication-overhead
bench and failover-time distribution over repeated kills, then the
process-boundary form of the failover eval (real `kill -9`, real processes;
the in-process abort's two softenings are flagged in NOTES §7), then LOG/STATUS/
NOTES/PDF milestone refresh.

## 2026-08-14 — Stage 4 COMPLETE: process-boundary kill -9 eval + the replication-cost and failover numbers

**Process harness.** New `src/harness/` library module: spawns the real
`replog_controller`/`replog_broker` binaries as child processes, parses their
listen line, SIGKILLs by broker id, restarts on the same data dir, and polls
controller metadata for cluster state. Library code on purpose — the Stage 4
process eval, the failover bench, and the Stage 5 torture harness all drive
clusters the same way. Nothing in-process survives a SIGKILL, which retires
the two softenings flagged in NOTES §7.

**Process-boundary failover eval green** (`tests/process_failover_tests.rs`,
now in the default suite): 3 broker *processes*, controller session 700 ms,
1200 records at acks=all through `ClusterClient`, real `kill -9` of the
leader process at record 400. Checker verdict: **1200/1200 acked ids
consumed, zero lost, zero duplicates this run, new leader elected.** The SPEC
Stage 4 pass condition now holds at the process boundary, not just in-process.

**Replication-overhead bench** (`bench/run_cluster_bench.sh` → `cluster_produce.csv`
+ `.png`; broker_bench grew `--rf` + `--acks all` with leader routing; fresh
3-broker cluster per row, localhost, 100 B values, fsync batch:1MiB:5ms):

- batch=100, RF=3, sync: acks=0 **528k**, acks=1 **208k**, acks=durable
  **5.3k**, acks=all **88k** rec/s.
- The Kafka lesson, measured on our own system: at batch=100, **acks=all is
  ~16× faster than acks=durable** (88k vs 5.3k) while giving the *stronger*
  practical guarantee (survives machine death, which an fsync does not).
  Durability by replication beats durability by disk barrier on this SSD by
  an order of magnitude.
- RF=1 baseline on the same binaries: acks=all 479k ≈ acks=written 416k —
  the acks=all *bookkeeping* is free; the cost is replication itself
  (208k→88k written→all at RF=3, a 2.4× tax).
- Honest finding: acks=all pipelining peaks at depth 2 (103k) and *degrades*
  to 60k at depth 8 (p99 27 ms) — three brokers + client share one CPU, so
  deeper pipelines only grow queues here. On separate machines (latency-
  dominated, not CPU-contended) depth should help; that measurement needs
  real hosts. Caveat recorded in NOTES §7.

**Failover-time distribution** (`failover_bench`, 12 real kill -9 of the
leader under continuous acks=all load, session timeout 700 ms): gap from kill
to first post-kill ack **min 2.17 s, p50 2.59 s, p90 2.65 s, max 2.66 s** —
tight, and ~0.8 s slower than the in-process eval's ~1.8 s single sample.
The extra is real-world plumbing the in-process form skipped: the client must
discover dead TCP connections, brokers learn the new epoch on their next
300 ms heartbeat, the new leader's HWM waits for the surviving follower's
first fetch of the new epoch, and the client retries on a 100 ms backoff.
Detection (700 ms) is still the largest single term, but the propagation
chain roughly triples it end to end — a good interview number precisely
because it is not just the timeout.

36 tests green. Next: Stage 5 torture harness (detailed plan now in PLAN.md).

## 2026-08-14 (later) — Stage 5 COMPLETE: the torture harness, two client-side scalps, and a clean soak

**Built** (per the detailed PLAN, scope signed off this morning): SplitMix64
RNG (hand-rolled, reference vectors — the seed is the experiment id),
`TcpProxy` with cut/heal (kills live connections, refuses new ones),
two-phase `ProcCluster` start so each broker dials the controller through its
own proxy, checker v2 (plain-text history files + gap-free-from-zero check),
and `replog_torture`: seeded schedule of `kill -9` and control-plane
partitions against real broker processes under continuous acks=all load, plus
an acks=1 "chaff" producer aimed at random brokers (zombies included) whose
ids are deliberately outside the contract, two independent readers per
partition, and an offline verdict — `replog_torture --verify <dir>` re-checks
any saved run with no cluster present.

**War story 1 — the zombie that poisoned the client.** The process-level
zombie test (cut the leader's controller link; it keeps serving while the
controller elects around it) wedged the producer for its whole 15 s retry
budget. Root cause in `ClusterClient::refresh_metadata`: first-successful-
answer-wins, and the deposed leader still answers metadata requests with its
stale view — itself as leader — from the front of the candidate list. Every
refresh re-poisoned the client; every produce parked against an HWM that
could never advance. Fix: metadata versions are monotonic at the controller,
so ask every reachable broker and keep the newest. Lesson: "first answer
wins" is "the fastest liar wins"; a version field nobody compares is
decoration.

**War story 2 — the divergence that refused to diverge.** The same test first
manufactured "divergence" by writing acks=1 to the zombie right after the
controller elected — and found all 20 records in the final *converged* log.
Nothing was broken: a control-plane cut leaves the data plane up, and until
each follower's next 300 ms heartbeat delivered the new epoch, their fetchers
were still replicating from the zombie — the writes became fully-replicated
legitimate history. "The leader changed" is one event *per observer*
(controller-elected, follower-adopted, client-visible); the test now waits
for the followers' own metadata views, at which point Stage 4's per-await
fencing is exactly what keeps an in-flight zombie fetch from landing.

**The verdicts** (`bench/run_torture.sh`, 3-broker cluster, RF=3, min-ISR 2,
700 ms session, 2 partitions, values 100 B):

- seed 1, 120 s: 30 faults (17 kills / 13 cuts), 5,095 acked — zero
  violations, 0 extra duplicates.
- seed 2, 120 s: 28 faults (9/19), 21,310 acked — zero violations, 30
  genuine retry duplicates counted.
- seed 3, 120 s: 28 faults (10/18), 20,180 acked — zero violations, 40 dups.
- **soak, seed 42, 15 min: 214 faults (107 kills / 107 cuts), 32,310 acked
  ids, every one consumed, offsets gap-free, both readers agree on every
  offset, 10 retry duplicates — zero contract violations.** Hours-long form:
  `SOAK_SECS=14400 bench/run_torture.sh`.

Availability was honestly ugly where it should be (seed 1 drew double the
kills and acked a quarter of seed 2's records — kills cost throughput, never
correctness), and duplicates appeared exactly where retries fired, counted,
never hidden. Determinism check: the 30 s shakedown and the 120 s run of
seed 1 produce identical schedule prefixes.

**43 tests green.** SPEC Stages 0–5 are all delivered: the durability
contract (`kill -9` at acks=all, zero acked loss) holds deterministically,
at the process boundary, and under seeded random schedules — verified by an
offline checker from client-observed histories in every form. Stage 6
(idempotent producer / compaction / sendfile / GCP deployment) remains the
documented stretch tier.
