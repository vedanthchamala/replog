# replog

A Kafka-style replicated commit log, built from scratch in Rust.

Partitioned append-only storage with CRC-protected records and torn-write crash
recovery, a custom binary protocol over TCP, consumer groups, leader/follower
replication with ISR tracking and leader-epoch fencing — and a fault-injection
harness whose checker proves the durability contract (`kill -9` the leader,
zero acknowledged writes lost) from client-observed histories.

**Status:** Stages 1–3 of 6 complete — storage engine, TCP broker/clients, and
partitioning + consumer groups (crash-triggered rebalance verified by a history
checker: 3000/3000 acked records delivered, duplicates counted, zero contract
violations). Next: Stage 4, replication + failover. See [SPEC.md](SPEC.md) for
guarantees and the stage plan, [PLAN.md](PLAN.md) for the active stage in detail,
[LOG.md](LOG.md) for the dated build log.

Built as a study of why real distributed logs are designed the way they are: every
major decision (fsync batching, sparse indexes, ISR vs quorum replication, epoch
fencing) is made explicitly, measured on stated hardware, and written up.

## Measured so far

The Stage 1 durability/latency curve (MacBook M4 Pro, APFS SSD, single-threaded;
reproduce with `bench/run_append_bench.sh`):

| fsync policy | 100 B values | 1 KiB values |
|---|---|---|
| `always` (F_FULLFSYNC per append) | 238 appends/s | 238 appends/s |
| `batch` (1 MiB / 50 ms group commit) | 550k appends/s | 110k appends/s |
| `os` (page cache decides) | 955k appends/s | 521k appends/s |

One true media barrier costs ~4.2 ms on this SSD — fsync-per-append caps the engine
at ~238 appends/s regardless of record size. Group commit amortizes one barrier over
~1 MiB of appends; the acks for those records are held until the covering flush.

The Stage 2 broker curve (one broker, one partition, localhost, 100 B values;
`bench/run_broker_bench.sh`) — produce throughput vs client batch size:

| records per batch | acks=written | acks=durable (held until covering flush) |
|---|---|---|
| 1 | 21.8k rec/s | 91 rec/s |
| 100 | 526k rec/s | 8.2k rec/s |
| 1000 | 553k rec/s | 56k rec/s |

Same amortization story one layer up: client batching splits the per-request cost
(and, at acks=durable, the group-commit barrier) across the batch. A `kill -9`
integration test verifies that every durable-acked record survives crash + restart
of a real broker process.

Three Stage 3 findings worth the click ([`bench/results/`](bench/results/)):
pipelining 8 produce batches makes durable acks nearly free (57k → 590k rec/s,
within ~5% of written — batches share the barrier); spreading one stream over
more partitions makes durable throughput *worse* on one disk (590k → 211k at 8
partitions — per-partition fsync fragments group commit into serialized device
barriers); and the ~600k rec/s single-machine ceiling belongs to the machine,
not any component (verified with per-partition connections and multiple client
processes). Partition scaling is a cross-machine story — measured at Stage 4.

```
cargo test                      # correctness suite (18 tests)
bench/run_append_bench.sh       # Stage 1: fsync-policy matrix → CSV
bench/run_broker_bench.sh       # Stage 2: batch/acks/pipelining matrix → CSV
uv run bench/plot_append.py     # CSVs → plots
uv run bench/plot_broker.py
```
