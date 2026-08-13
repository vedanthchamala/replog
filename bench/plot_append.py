# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib"]
# ///
"""Plot the fsync-policy curve from bench/results/append_fsync.csv.

Usage: uv run bench/plot_append.py [in.csv] [out.png]
"""

import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt

IN = Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results/append_fsync.csv")
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else "bench/results/append_fsync.png")

rows = list(csv.DictReader(IN.open()))
sizes = sorted({int(r["value_bytes"]) for r in rows})
policies = list(dict.fromkeys(r["policy"] for r in rows))

LABELS = {"always": "always\n(F_FULLFSYNC per append)", "os": "os\n(page cache decides)"}


def label(p: str) -> str:
    if p.startswith("batch:"):
        _, b, ms = p.split(":")
        return f"batch\n({int(b) // 1024} KiB / {ms} ms)"
    return LABELS.get(p, p)


fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
width = 0.8 / len(sizes)
colors = ["#4878a8", "#e49444"]

for i, size in enumerate(sizes):
    by_policy = {r["policy"]: r for r in rows if int(r["value_bytes"]) == size}
    xs = [j + i * width for j in range(len(policies))]
    tput = [float(by_policy[p]["appends_per_sec"]) for p in policies]
    p99 = [float(by_policy[p]["p99_us"]) for p in policies]
    ax1.bar(xs, tput, width, label=f"{size} B values", color=colors[i % len(colors)])
    ax2.bar(xs, p99, width, label=f"{size} B values", color=colors[i % len(colors)])
    for x, v in zip(xs, tput):
        ax1.annotate(f"{v:,.0f}", (x, v), ha="center", va="bottom", fontsize=8)
    for x, v in zip(xs, p99):
        ax2.annotate(f"{v:,.0f}", (x, v), ha="center", va="bottom", fontsize=8)

mid = (len(sizes) - 1) * width / 2
for ax, title, ylab in (
    (ax1, "Append throughput by fsync policy", "appends/sec (log)"),
    (ax2, "p99 append latency by fsync policy", "p99 latency, µs (log)"),
):
    ax.set_yscale("log")
    ax.set_xticks([j + mid for j in range(len(policies))])
    ax.set_xticklabels([label(p) for p in policies], fontsize=9)
    ax.set_title(title, fontsize=11)
    ax.set_ylabel(ylab)
    ax.legend(fontsize=8)
    ax.margins(y=0.18)

fig.suptitle("replog Stage 1 — durability/latency curve (MacBook M4 Pro, APFS)", fontsize=12)
fig.tight_layout()
fig.savefig(OUT, dpi=150)
print(f"wrote {OUT}")
