# STATUS

> Session pickup file. Read SPEC.md → PLAN.md → this file → LOG.md (latest entry) at
> the start of every session, before touching code.

**Stage:** 2 — TCP broker + clients (planning → building)
**Last updated:** 2026-08-13

## Done

- Rust toolchain installed (rustup stable, minimal profile).
- Repo initialized, public on GitHub (`vedanthchamala/replog`).
- SPEC.md (guarantees, non-goals, stage plan), PLAN.md (per-stage detail).
- **Stage 1 COMPLETE** — storage engine (CRC'd records, sparse indexes, segment
  roll, torn-tail recovery, fsync policies with F_FULLFSYNC on macOS), 8/8 tests
  green, and the fsync-policy curve measured + plotted (`bench/results/`):
  always 238 appends/s vs batch 550k vs os 955k at 100 B values.

## In progress

- Stage 2: detail the plan in PLAN.md, then build wire protocol + tokio broker +
  producer/consumer clients.

## Next actions (in order)

1. PLAN.md: Stage 2 detailed (frame format, message types, broker task model,
   client API, tests, bench).
2. Build `proto` module (frames + message encode/decode, round-trip tests).
3. Build broker (per-partition writer task, ack-on-covering-flush) + clients.
4. Stage 2 evals: e2e produce→fetch, offset resume after restart; bench
   throughput vs batch size + latency percentiles.
5. Stage 2 milestone: first pdflatex compile of interview/replog_guide.tex.

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
