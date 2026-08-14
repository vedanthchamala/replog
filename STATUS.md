# STATUS

> Session pickup file. Read SPEC.md → PLAN.md → this file → LOG.md (latest entry) at
> the start of every session, before touching code.

**Stage:** 4 COMPLETE (process-boundary eval + bench done) → Stage 5 build (torture harness; detailed plan in PLAN.md, scope signed off 2026-08-14)
**Last updated:** 2026-08-14

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

## In progress

- Stage 5 build: torture harness per the now-detailed PLAN (seeded scheduler,
  control-plane partitions by proxy, checker v2, `replog_torture` bin).

## Next actions (in order)

1. Stage 5 components: `harness/rng.rs` (SplitMix64), `harness/proxy.rs`
   (TcpProxy with cut/heal), ProcCluster two-phase start (per-broker
   controller addr), checker v2 (save/load + gap check).
2. `replog_torture` bin (seeded fault loop, acks=all + chaff workload, two
   readers per partition, drain + verify) + `bench/run_torture.sh` matrix.
3. Stage eval: 3 seeds x 120 s checker-clean + one >=10 min soak in-session;
   violations become LOG war stories.
4. Stage 5 milestone: LOG + STATUS + NOTES + PDF refresh + push.

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
