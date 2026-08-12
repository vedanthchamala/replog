# replog — a replicated commit log, from scratch

A Kafka-style distributed log built from first principles in Rust: brokers holding
partitioned append-only logs on disk, producers and consumers speaking a custom binary
protocol over TCP, leader/follower replication with configurable acknowledgement levels,
and a fault-injection harness that proves acknowledged writes survive `kill -9`.

The goal is not to compete with Kafka. It is to build the *core* of a durable, replicated
log well enough that every design decision inside a real one (fsync batching, sparse
indexes, ISR replication, leader epochs, rebalance protocols) has been made — and
measured — by hand.

---

## What the finished system guarantees

1. **Durability contract.** A write acknowledged at `acks=all` survives the crash
   (`kill -9`) of any single broker, verified by an external checker, not by assertion.
2. **Ordering.** Within one partition, consumers observe records in offset order with
   no gaps and no reordering. (Across partitions: no ordering claim, same as Kafka.)
3. **At-least-once delivery** end to end; duplicates are possible on retry and are
   counted honestly by the checker (exactly-once/idempotent producer is a stretch goal).
4. **Crash consistency of the storage engine.** A broker killed mid-write recovers by
   truncating a torn tail; it never serves a corrupt record (CRC-verified) and never
   loses a record it fsynced.

## Non-goals (deliberate)

- **Kafka wire-protocol compatibility.** Real Kafka clients cannot connect. The wire
  protocol is our own; implementing Kafka's is a compatibility slog that teaches
  serialization trivia, not distributed systems.
- **Raft for the data path.** Replication is Kafka-style leader/ISR, not Raft.
  The metadata/controller plane is a single-controller simplification (KRaft-lite).
  Knowing *why* Kafka chose ISR over quorum replication for the data path is part of
  the point.
- **Tiered storage, compaction-as-GC, transactions, TLS/SASL, multi-datacenter.** Out
  of scope. Log compaction is a stretch goal at best.
- **Beating Kafka on benchmarks.** All numbers are measured against this system's own
  baselines (fsync policies, batch sizes, replication factors) on stated hardware.
  Any comparison to real Kafka is labeled as what it is: one laptop, defaults, honest.

---

## Architecture at a glance

```
 producer ──┐                        ┌── consumer (group member)
 producer ──┤   TCP, length-prefixed ├── consumer (group member)
            ▼   binary protocol      ▼
   ┌─────────────────────────────────────────┐
   │ broker                                   │
   │  ┌────────────┐  ┌─────────────────┐    │
   │  │ connection │→ │ partition router │    │
   │  │ layer      │  └───────┬─────────┘    │
   │  └────────────┘          ▼              │
   │            ┌──────────────────────┐     │
   │            │ log (per partition)  │     │
   │            │  segments + index    │     │
   │            │  fsync policy        │     │
   │            └──────────────────────┘     │
   │  replication: leader ⇄ follower fetch   │
   └─────────────────────────────────────────┘
        cluster: N brokers + 1 controller (metadata, leader election)
```

- **Storage engine** (Stage 1): per-partition log = directory of fixed-size *segments*
  (`<base_offset>.log` + sparse `.index`). Records are length-prefixed, CRC-protected.
  Fsync policy is explicit and configurable: `always` / batched group commit / OS-decided.
- **Network layer** (Stage 2): tokio broker; length-prefixed frames; produce/fetch with
  client-side batching and pipelining; consumer offsets stored in an internal log.
- **Partitioning + consumer groups** (Stage 3): key-hash partitioning; broker-coordinated
  group membership; rebalance on join/leave/death.
- **Replication** (Stage 4): per-partition leader + followers; followers fetch like
  consumers; in-sync-replica (ISR) set tracked by the leader; `acks=0|1|all`; controller
  elects leaders; leader-epoch fencing so a rejoining stale leader truncates divergence
  instead of splitting the log.
- **Torture harness** (Stage 5): workload generator + fault injector (`kill -9`,
  partition-by-proxy, disk-full) + an offline history checker that verifies the
  durability/ordering contract from client-observed histories alone.

## Stage plan and pass conditions

Each stage ends in a working demo, a measured result, and a LOG.md entry. A stage is
not done until its pass conditions all hold.

| Stage | Deliverable | Pass conditions |
|---|---|---|
| 0 | Toolchain + scaffold | `cargo test` green in CI-able form; repo public |
| 1 | Single-node storage engine | Torn-write recovery test: kill mid-append, reopen, no acked-fsynced record lost, tail truncated cleanly. CRC catches injected corruption. Bench: append throughput at `fsync=always` vs group-commit vs `os` — the durability/latency curve, plotted. |
| 2 | TCP broker + clients | End-to-end produce→fetch; consumer resumes from committed offset after restart; bench: throughput vs batch size, latency percentiles at fixed rate. |
| 3 | Partitions + groups | 2 consumers split N partitions; kill one → rebalance; every record delivered ≥ once, verified by checker. |
| 4 | Replication + failover | `kill -9` the leader mid-stream at `acks=all` → new leader elected, **zero acked records lost** (checker-verified); stale-leader rejoin truncates via epoch check; measured: failover time, replication overhead at acks=0/1/all. |
| 5 | Torture harness | Randomized fault schedule (seeded, reproducible) over hours: checker finds zero contract violations; every violation found during development is documented in LOG.md with root cause and fix. |
| 6 (stretch) | One or more of: idempotent producer, log compaction, sendfile zero-copy reads, 3-broker GCP deployment | Each with its own measured before/after. |

## Measurement discipline

- Every performance claim states hardware, workload, and what the baseline is.
- Benchmarks are scripted and re-runnable (`bench/`), output to CSV, tables/plots
  generated from the CSVs — numbers on the page trace to a command.
- Correctness claims come from the checker or the test suite, never from "it looked fine."

## Tech

Rust (stable), tokio (from Stage 2), `crc32fast`, `thiserror`; no serde on the wire or
disk path (hand-rolled binary formats are the point); pytest-style integration tests via
`cargo test` + a small Python harness later for multi-process orchestration if needed.
Hardware for benchmarks: MacBook M4 Pro (local), GCP e2 VMs (multi-node, Stage 4+).
