# STATUS

> Session pickup file. Read SPEC.md → PLAN.md → this file → LOG.md (latest entry) at
> the start of every session, before touching code.

**Stage:** 2 complete → 3 (partitions + consumer groups) next
**Last updated:** 2026-08-13

## Done

- Rust toolchain installed (rustup stable, minimal profile).
- Repo initialized, public on GitHub (`vedanthchamala/replog`).
- SPEC.md (guarantees, non-goals, stage plan), PLAN.md (per-stage detail).
- **Stage 1 COMPLETE** — storage engine (CRC'd records, sparse indexes, segment
  roll, torn-tail recovery, fsync policies with F_FULLFSYNC on macOS), 8/8 tests
  green, and the fsync-policy curve measured + plotted (`bench/results/`):
  always 238 appends/s vs batch 550k vs os 955k at 100 B values.
- **Stage 2 COMPLETE** — wire protocol (frames, correlation IDs), tokio broker
  (per-partition writer threads, durable acks held until covering flush, long-poll
  fetch, `__offsets` log), pipelined clients. 18 tests green incl. `kill -9` of a
  real broker process with zero durable-acked loss. Bench: batch curve 21.8k→553k
  rec/s written / 91→56k durable; pipelining to ~646k; open-loop percentiles.
  War story: the double-fsync (two timers, one flush) — LOG 2026-08-13.

## In progress

- Interview PDF first compile (Stage 2 milestone).

## Next actions (in order)

1. interview/replog_guide.tex: compile NOTES.md material with pdflatex.
2. PLAN.md: detail Stage 3 (key-hash partitioning, group coordinator,
   join/leave/heartbeat, generation fencing, rebalance, checker v1).
3. Build Stage 3 + its evals (2 consumers split partitions; kill one → rebalance;
   at-least-once delivery verified by checker).
4. Stage 3 milestone: LOG entry + STATUS + PDF refresh + push.

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
