# STATUS

> Session pickup file. Read SPEC.md → PLAN.md → this file → LOG.md (latest entry) at
> the start of every session, before touching code.

**Stage:** 3 complete → 4 (replication + failover) next
**Last updated:** 2026-08-13

## Done

- Rust toolchain installed (rustup stable, minimal profile).
- Repo initialized, public on GitHub (`vedanthchamala/replog`).
- SPEC.md (guarantees, non-goals, stage plan), PLAN.md (per-stage detail).
- **Stage 1 COMPLETE** — storage engine (CRC'd records, sparse indexes, torn-tail
  recovery, fsync policies with F_FULLFSYNC), 8 tests, fsync curve measured:
  always 238 vs batch 550k vs os 955k appends/s at 100 B.
- **Stage 2 COMPLETE** — wire protocol (frames, correlation IDs), tokio broker
  (partition writer threads, durable acks held until covering flush, long-poll,
  `__offsets`), pipelined clients. `kill -9` durability test. Batch curve 21.8k→553k
  written / 91→57k durable rec/s. War story: the double-fsync (LOG 2026-08-13).
- **Stage 2 milestone PDF** — `interview/replog_guide.tex` compiles (7 pages).
- **Stage 3 COMPLETE** — key-hash partitioning, broker-side group coordinator
  (generations, range assignment, eviction sweep), fenced commits, GroupConsumer,
  checker v1. Eval: consumer crash → rebalance, 3000/3000 delivered, 400 dups
  counted, takeover 1.16 s. Fan-out findings: durable+pipelining ≈ written (590k);
  per-partition fsync fragments group commit (590k→211k at 8 partitions);
  single-machine ceiling ~600k rec/s is the machine. 28 tests green.

## In progress

- (nothing — Stage 4 planning is next)

## Next actions (in order)

1. PLAN.md: detail Stage 4 (controller, follower fetch, ISR tracking, HWM,
   acks=all, min-ISR, leader epochs, failover demo + measurements).
2. Build Stage 4 + evals (kill -9 leader under load at acks=all → zero acked
   loss, checker-verified; stale-leader rejoin truncates via epoch).
3. Stage 4 milestone: LOG + STATUS + NOTES + PDF refresh + push.
4. Stage 5: torture harness (seeded fault schedules, offline checker).

## Standing rules for any session (any model)

- Commits: conventional prefixes (feat:/fix:/docs:/chore:), authored by Vedanth's git
  identity ONLY — **no AI co-author trailers, ever, in this repo.**
- Every working-session ends with: tests run + output shown, LOG.md dated entry,
  STATUS.md refreshed, commit + push.
- `interview/` is git-ignored and private: running decision/war-story notes that feed
  the interview-prep PDF (built with pdflatex at stage milestones). Update it whenever
  a design decision is made or a bug is fixed — it is the PDF's source material.
- Honesty discipline: no claim without a command that reproduces it. Accuracy caveats
  recorded in interview/NOTES.md as they arise (what NOT to overclaim later).
