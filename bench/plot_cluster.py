# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib"]
# ///
"""Plot the Stage 4 replication-overhead and failover curves.

Usage: uv run bench/plot_cluster.py [produce.csv] [gaps.csv] [out.png]
"""

import csv
import statistics
import sys
from pathlib import Path

import matplotlib.pyplot as plt

IN = Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results/cluster_produce.csv")
GAPS = Path(sys.argv[2] if len(sys.argv) > 2 else "bench/results/failover_gaps.csv")
OUT = Path(sys.argv[3] if len(sys.argv) > 3 else "bench/results/cluster_produce.png")

rows = list(csv.DictReader(IN.open()))
gaps = list(csv.DictReader(GAPS.open()))
rf3 = [r for r in rows if r["rf"] == "3" and r["inflight"] == "1"]
b100 = {(r["acks"], r["rf"]): r for r in rows if r["batch_records"] == "100" and r["inflight"] == "1"}

COLORS = {"none": "#8a8a8a", "written": "#4878a8", "durable": "#e49444", "all": "#59935b"}

fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(11.5, 8.4))

for acks in ("none", "written", "durable", "all"):
    pts = sorted(
        (int(r["batch_records"]), float(r["records_per_sec"]), float(r["p99_batch_us"]))
        for r in rf3
        if r["acks"] == acks
    )
    xs = [p[0] for p in pts]
    ax1.plot(xs, [p[1] for p in pts], "o-", color=COLORS[acks], label=f"acks={acks}")
    if acks != "none":
        ax2.plot(xs, [p[2] for p in pts], "o-", color=COLORS[acks], label=f"acks={acks}")

ax1.set_title("Throughput vs batch size, RF=3\n(synchronous, 1 partition)", fontsize=10)
ax1.set_ylabel("records/sec (log)")
ax2.set_title("p99 batch-ack latency vs batch size, RF=3", fontsize=10)
ax2.set_ylabel("p99 latency, µs (log)")
for ax in (ax1, ax2):
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("records per Produce batch")
    ax.set_xticks([1, 100, 1000], ["1", "100", "1000"])
    ax.legend(fontsize=9)
    ax.grid(True, which="both", alpha=0.25)

# The replication tax at batch=100: same binaries, RF=1 vs RF=3.
acks_levels = ("written", "durable", "all")
x = range(len(acks_levels))
w = 0.35
for i, (rf, shade) in enumerate((("1", 0.45), ("3", 1.0))):
    vals = [float(b100[(a, rf)]["records_per_sec"]) if (a, rf) in b100 else 0 for a in acks_levels]
    ax3.bar(
        [xi + (i - 0.5) * w for xi in x],
        vals,
        w,
        color=[COLORS[a] for a in acks_levels],
        alpha=shade,
        label=f"RF={rf}",
    )
    for xi, v in zip(x, vals):
        ax3.text(xi + (i - 0.5) * w, v, f"{v/1e3:.0f}k", ha="center", va="bottom", fontsize=8)
ax3.set_title("The replication tax (batch=100)\nfaded = RF=1 baseline, solid = RF=3", fontsize=10)
ax3.set_xticks(list(x), [f"acks={a}" for a in acks_levels])
ax3.set_ylabel("records/sec")
ax3.set_yscale("log")
ax3.grid(True, axis="y", alpha=0.25)

xs = [int(g["kill"]) for g in gaps]
ys = [float(g["gap_ms"]) / 1e3 for g in gaps]
med = statistics.median(ys)
ax4.plot(xs, ys, "o", color="#a85454")
ax4.axhline(med, color="#a85454", ls="--", lw=1, label=f"median {med:.2f} s")
ax4.axhline(0.7, color="#777", ls=":", lw=1, label="liveness timeout 0.7 s")
ax4.set_title(
    "Failover: kill -9 the leader → first acks=all ack\n(real broker processes, repeated kills)",
    fontsize=10,
)
ax4.set_xlabel("kill #")
ax4.set_ylabel("client-visible gap, seconds")
ax4.set_ylim(bottom=0)
ax4.legend(fontsize=9)
ax4.grid(True, alpha=0.25)

fig.suptitle(
    "replog Stage 4 — replication + failover, 3-broker cluster "
    "(MacBook M4 Pro, localhost, 100 B values, fsync batch:1MiB:5ms, min_isr=2)",
    fontsize=11,
)
fig.tight_layout()
fig.savefig(OUT, dpi=150)
print(f"wrote {OUT}")
