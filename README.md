# replog

A Kafka-style replicated commit log, built from scratch in Rust.

Partitioned append-only storage with CRC-protected records and torn-write crash
recovery, a custom binary protocol over TCP, consumer groups, leader/follower
replication with ISR tracking and leader-epoch fencing — and a fault-injection
harness whose checker proves the durability contract (`kill -9` the leader,
zero acknowledged writes lost) from client-observed histories.

**Status:** Stage 2 of 6 — TCP broker + clients. Stage 1 (single-node storage
engine) is complete. See [SPEC.md](SPEC.md) for guarantees and the stage plan,
[PLAN.md](PLAN.md) for the active stage in detail, [LOG.md](LOG.md) for the dated
build log.

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
Details and the full CSV/plot: [`bench/results/`](bench/results/).

```
cargo test                    # correctness suite
bench/run_append_bench.sh     # fsync-policy matrix → CSV
uv run bench/plot_append.py   # CSV → plot
```
