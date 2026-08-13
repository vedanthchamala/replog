# STATUS

> Session pickup file. Read SPEC.md → PLAN.md → this file → LOG.md (latest entry) at
> the start of every session, before touching code.

**Stage:** 4 core complete (evals green) → Stage 4 close-out (bench + process-boundary eval)
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
- **Stage 4 CORE** — controller (liveness, ISR-only elections, fsynced snapshots),
  replica fetchers with epoch reconciliation, ISR/HWM tracking, acks=all ledger,
  `ClusterClient` (leader routing, retry-through-failover), four evals green:
  failover at acks=all loses zero of 1200 acked ids (first post-kill ack ~1.8 s),
  logs converge byte-identical, acks=all refused below min-ISR, stale leader
  truncates via epoch check. Three bugs found and fixed on the way (LOG
  2026-08-13: parked-waiter connection hang, per-await fencing, ISR
  caught-up-recency rule). 35 tests green.

## In progress

- Stage 4 close-out: measurements + process-boundary eval remain (see next).

## Next actions (in order)

1. Stage 4 bench per PLAN: produce throughput/latency at acks=0/1/all on the
   3-broker localhost cluster (replication overhead curve) + failover-time
   distribution over repeated kills (`bench/`, CSV + plot).
2. Process-boundary failover eval: broker as child processes, real `kill -9`
   (the current eval is the in-process form; softenings flagged in NOTES §7).
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
