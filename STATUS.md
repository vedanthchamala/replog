# STATUS

> Session pickup file. Read SPEC.md → PLAN.md → this file → LOG.md (latest entry) at
> the start of every session, before touching code.

**Stage:** 6 CORE DELIVERED — the torture harness is now a cross-system,
target-agnostic fault-injection tool (Docker kill/pause/isolate backend,
rdkafka + replog workload adapters, seeded schedule + offline checker held
constant). replog and Redpanda run identical seeded schedules in identical
containers at a matched 1000 ms detection timeout. All THREE targets —
replog, Redpanda, and Apache Kafka (KRaft) — hold the contract (zero acked
loss); the harness surfaced a real availability defect in replog's own
acks=all leader-failover path, and running Kafka (also ISR) localized it as
a replog-specific bug rather than an ISR trade-off (Kafka recovers on
election; replog waits for the killed broker to rejoin).
**Last updated:** 2026-09-09

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
- **Stage 4 COMPLETE (2026-08-14)** — `src/harness/` process-cluster module
  (real binaries, SIGKILL, restart-on-data-dir); process-boundary failover
  eval green (1200/1200 acked survive a real `kill -9`, checker-verified);
  replication bench (`run_cluster_bench.sh`): at batch=100 RF=3 acks
  0/1/durable/all = 528k/208k/5.3k/88k rec/s — acks=all beats fsync-durability
  16×; RF=1 acks=all ≈ written (bookkeeping free); failover distribution over
  12 real kills: p50 2.59 s, max 2.66 s. 36 tests green.
- **Stage 5 COMPLETE (2026-08-14)** — torture harness: SplitMix64 seeded
  scheduler, TcpProxy control-plane partitions (live zombie leaders),
  checker v2 (history files + gap check, offline `--verify`),
  `replog_torture` bin + `bench/run_torture.sh`. 3 seeds × 120 s + 15-min
  soak (214 faults, 32,310 acked ids): zero contract violations everywhere,
  duplicates counted. Two war stories: client stale-metadata poisoning by a
  zombie (fixed: newest-version-wins refresh) and the
  divergence-that-refused-to-diverge lesson (LOG 2026-08-14). Process-level
  zombie/stale-leader eval now in the suite. 43 tests green.

## In progress

- Nothing mid-edit. Stage 6 core is committed; working tree builds, 43 tests green.

## Next actions (in priority order)

1. **Fix the failover availability defect the harness found** (the standout item).
   replog's acks=all recovery after a leader `kill` scales with broker downtime
   (heal 12 s → ~15 s ack gap) because a newly-elected leader re-admits the
   *dead* old leader to the ISR via the caught-up-recency grace window, gating
   the HWM on a dead broker's frozen offset. Fix: on a follower→leader
   transition, do not extend the in-sync grace to a replica the controller has
   marked down (or seed `match_offsets`/`leader_since` per replica from its last
   known liveness). Durability is unaffected (checker-clean); this is
   availability. Re-run `bench/run_faults.sh replog` and confirm the
   recovery-vs-downtime line flattens toward Redpanda's.
2. Longer torture soaks anytime (Stage 5 harness): `SOAK_SECS=14400 bench/run_torture.sh`.
4. Remaining Stage 6-stretch ideas if desired: idempotent producer, compaction,
   sendfile zero-copy, GCP cross-zone numbers.

## Stage 6 how-to (so a fresh session can reproduce)

- Bring up targets: `deploy/redpanda/up.sh` and `NO_BUILD=1 deploy/replog/up.sh`
  (first replog run needs the image: `deploy/replog/up.sh` builds it). Both use a
  shared `peers` network + one `edge-N` network per broker.
- Run the matrix: `bench/run_faults.sh <redpanda|replog> [seeds...]` — probe,
  baseline, acks=1/acks=all controls, then the seeds. Results in
  `bench/results/faults/<target>/` (history.txt is git-ignored; csvs/logs/
  summaries/plots are kept).
- If a run is interrupted mid-fault: `deploy/heal.sh <target>` restores
  running/unpaused/attached containers.
- Plots: `uv run bench/plot_faults.py` (tables + ECDFs), `uv run
  bench/plot_recovery.py` (recovery-vs-downtime).
- The `kafka` feature is required: `cargo build --release --features kafka`.

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
