# STATUS

> Session pickup file. Read SPEC.md → PLAN.md → this file → LOG.md (latest entry) at
> the start of every session, before touching code.

**Stage:** 1 — single-node storage engine (ACTIVE)
**Last updated:** 2026-08-12

## Done

- Rust toolchain installed (rustup stable, minimal profile).
- Repo initialized, public on GitHub (`vedanthchamala/replog`).
- SPEC.md (guarantees, non-goals, stage plan), PLAN.md (Stage 1 detailed).
- **Stage 1 storage engine core: DONE, 8/8 tests green** — CRC'd record format,
  sparse indexes, segment roll, torn-tail recovery (every-byte-cut property test),
  loud corruption errors, fsync policies (always / group-commit / os) with
  F_FULLFSYNC on macOS, durable-offset tracking.

## In progress

- Stage 1 remainder: `bench/` append benchmark (the fsync-policy curve).

## Next actions (in order)

1. `bench/` append benchmark: fsync=always vs batch vs os → CSV + plot → LOG.md entry.
2. Stage 1 exit review against SPEC pass conditions; mark stage complete.
3. Detail Stage 2 in PLAN.md (tokio broker + wire protocol), then build it.
4. At the Stage 2 milestone: first compile of the interview PDF from interview/NOTES.md.

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
