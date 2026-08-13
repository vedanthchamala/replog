# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib"]
# ///
"""Plot the Stage 2/3 broker produce curves from bench/results/broker_produce.csv.

Usage: uv run bench/plot_broker.py [in.csv] [out.png]
"""

import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt

IN = Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results/broker_produce.csv")
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else "bench/results/broker_produce.png")

rows = list(csv.DictReader(IN.open()))
closed = [r for r in rows if r["target_rate"] == "0"]
sync = [r for r in closed if r["inflight"] == "1" and r["partitions"] == "1"]
sweep = [
    r for r in closed
    if r["batch_records"] == "100" and r["acks"] == "written" and r["partitions"] == "1"
]

COLORS = {"written": "#4878a8", "durable": "#e49444"}

fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(11.5, 8.4))

for acks in ("written", "durable"):
    pts = sorted(
        ((int(r["batch_records"]), float(r["records_per_sec"]), float(r["p99_batch_us"]))
         for r in sync if r["acks"] == acks)
    )
    xs = [p[0] for p in pts]
    ax1.plot(xs, [p[1] for p in pts], "o-", color=COLORS[acks], label=f"acks={acks}")
    ax2.plot(xs, [p[2] for p in pts], "o-", color=COLORS[acks], label=f"acks={acks}")

ax1.set_title("Throughput vs producer batch size\n(synchronous, 1 partition)", fontsize=10)
ax1.set_ylabel("records/sec (log)")
ax2.set_title("p99 batch-ack latency vs batch size\n(synchronous, 1 partition)", fontsize=10)
ax2.set_ylabel("p99 latency, µs (log)")
for ax in (ax1, ax2):
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("records per Produce batch")
    ax.set_xticks([1, 10, 100, 1000], ["1", "10", "100", "1000"])
    ax.legend(fontsize=9)
    ax.grid(True, which="both", alpha=0.25)

by_inflight: dict[int, float] = {}
for r in sweep:
    by_inflight.setdefault(int(r["inflight"]), float(r["records_per_sec"]))
pts = sorted(by_inflight.items())
ax3.plot([p[0] for p in pts], [p[1] for p in pts], "o-", color="#4878a8")
ax3.set_title(
    "Throughput vs pipelining depth\n(batch=100, acks=written, 1 partition)", fontsize=10
)
ax3.set_xlabel("requests in flight")
ax3.set_ylabel("records/sec")
ax3.set_xscale("log", base=2)
ax3.set_xticks([1, 2, 4, 8, 16], ["1", "2", "4", "8", "16"])
ax3.set_ylim(bottom=0)
ax3.grid(True, alpha=0.25)

# Partition fan-out: shared connection, pipelining depth 8 total.
series = [
    ("written", "1", "written, shared conn"),
    ("durable", "1", "durable, shared conn"),
]
for acks, _, label in series:
    pts = sorted(
        (int(r["partitions"]), float(r["records_per_sec"]))
        for r in closed
        if r["acks"] == acks and r["inflight"] == "8" and r["connections"] == "1"
        and r["batch_records"] == ("100" if acks == "written" else "1000")
    )
    ax4.plot(
        [p[0] for p in pts],
        [p[1] for p in pts],
        "o-",
        color=COLORS[acks],
        label=label,
    )
cpp = sorted(
    [(1, next(float(r["records_per_sec"]) for r in closed
              if r["acks"] == "durable" and r["inflight"] == "8"
              and r["connections"] == "1" and r["partitions"] == "1"
              and r["batch_records"] == "1000"))]
    + [
        (int(r["partitions"]), float(r["records_per_sec"]))
        for r in closed
        if r["acks"] == "durable" and r["batch_records"] == "1000"
        and r["connections"] == r["partitions"] and r["partitions"] != "1"
    ]
)
ax4.plot(
    [p[0] for p in cpp],
    [p[1] for p in cpp],
    "s--",
    color="#b0651a",
    label="durable, conn/partition, deeper pipeline",
)
ax4.set_title(
    "Partition fan-out on ONE disk\n(group commit fragments across writers)", fontsize=10
)
ax4.set_xlabel("partitions (one writer thread each)")
ax4.set_ylabel("records/sec")
ax4.set_xscale("log", base=2)
ax4.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
ax4.set_ylim(bottom=0)
ax4.legend(fontsize=8)
ax4.grid(True, alpha=0.25)

fig.suptitle(
    "replog Stages 2–3 — produce over TCP, one broker "
    "(MacBook M4 Pro, localhost, 100 B values, fsync batch:1MiB:5ms)",
    fontsize=11,
)
fig.tight_layout()
fig.savefig(OUT, dpi=150)
print(f"wrote {OUT}")
