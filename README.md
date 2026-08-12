# replog

A Kafka-style replicated commit log, built from scratch in Rust.

Partitioned append-only storage with CRC-protected records and torn-write crash
recovery, a custom binary protocol over TCP, consumer groups, leader/follower
replication with ISR tracking and leader-epoch fencing — and a fault-injection
harness whose checker proves the durability contract (`kill -9` the leader,
zero acknowledged writes lost) from client-observed histories.

**Status:** Stage 1 of 6 — single-node storage engine. See [SPEC.md](SPEC.md) for
guarantees and the stage plan, [PLAN.md](PLAN.md) for the active stage in detail,
[LOG.md](LOG.md) for the dated build log.

Built as a study of why real distributed logs are designed the way they are: every
major decision (fsync batching, sparse indexes, ISR vs quorum replication, epoch
fencing) is made explicitly, measured on stated hardware, and written up.

```
cargo test        # correctness suite
cargo bench ...   # (per stage) measured claims trace to a command
```
