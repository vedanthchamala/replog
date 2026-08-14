# replog

A Kafka-style replicated commit log, built from scratch in Rust.

Partitioned append-only storage with CRC-protected records and torn-write crash
recovery, a custom binary protocol over TCP, consumer groups, leader/follower
replication with ISR tracking and leader-epoch fencing — and a fault-injection
harness whose checker proves the durability contract (`kill -9` the leader,
zero acknowledged writes lost) from client-observed histories.

**Status:** Stages 1–5 of 6 complete — storage engine, TCP broker/clients,
partitioning + consumer groups, leader/ISR replication with epoch-fenced
failover, and the torture harness. The SPEC durability contract holds at the
process boundary: `kill -9` the leader mid-stream at `acks=all` → new leader
elected, **1200/1200 acked records survive**, checker-verified — and seeded
random fault schedules (leader kills, zombie-leader partitions) stay
checker-clean. Stage 6 items (idempotent producer, compaction, zero-copy
fetch) are stretch. See [SPEC.md](SPEC.md) for guarantees and the stage plan,
[PLAN.md](PLAN.md) for per-stage detail, [LOG.md](LOG.md) for the dated build
log including every bug found and fixed along the way.

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
processes).

The Stage 4 replication curve (3-broker cluster + controller, localhost,
batch=100, `bench/run_cluster_bench.sh`):

| acks | RF=3 | RF=1 baseline |
|---|---|---|
| 0 (none) | 528k rec/s | — |
| 1 (written) | 208k rec/s | 416k rec/s |
| 2 (durable, fsync) | 5.3k rec/s | 7.7k rec/s |
| all (ISR-replicated) | **88k rec/s** | 479k rec/s |

The headline: **`acks=all` is ~16× faster than fsync-durability** while
protecting against strictly more (machine death, not just process death) — a
network copy costs microseconds, a media barrier costs ~4 ms. That asymmetry
is *the* reason Kafka's default path replicates but does not fsync per batch.
Failover, measured over 12 repeated real `kill -9`s of the leader: the
client-visible gap is p50 **2.59 s** at a 700 ms liveness timeout — detection
is only ~27% of it; the rest is the election/metadata/retry propagation chain.

Stage 5 (`bench/run_torture.sh`): seeded random fault schedules — `kill -9`
and control-plane partitions that leave a deposed leader live and serving
(the zombie scenario) — against a real 3-broker cluster under `acks=all`
load. Every run to date is checker-clean from client-observed histories
alone (e.g. seed 2: 28 faults, 21,310 acked ids, every one consumed, 30
genuine retry duplicates counted, offsets gap-free, two independent readers
in full agreement). Building the harness caught a real client bug — metadata
refresh taking the first answer let a zombie keep routing the client to
itself; fixed by comparing metadata versions ([LOG.md](LOG.md)).

```
cargo test                      # correctness suite, incl. process-boundary kill -9 evals
bench/run_append_bench.sh       # Stage 1: fsync-policy matrix → CSV
bench/run_broker_bench.sh       # Stage 2/3: batch/acks/pipelining/partition matrix
bench/run_cluster_bench.sh      # Stage 4: replication overhead + failover distribution
bench/run_torture.sh            # Stage 5: seeded fault schedules, checker-verified
uv run bench/plot_append.py     # CSVs → plots
uv run bench/plot_broker.py
uv run bench/plot_cluster.py
```
