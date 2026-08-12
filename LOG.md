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
