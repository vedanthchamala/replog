# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib"]
# ///
"""Flagship chart: acks=all recovery time vs. broker downtime, per target.

Every leader-fault row across all runs of a target is a point (heal_after_ms,
max_gap_ms). The diagonal y=x is "recovered exactly when the broker came back".
A target on the diagonal waits for the killed broker to rejoin; a target on a
flat line recovers on election, independent of how long the broker stays down.

Usage: uv run bench/plot_recovery.py [results_root] [out.png]
"""
import csv
import sys
from pathlib import Path
from collections import defaultdict
import matplotlib.pyplot as plt

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results/faults")
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else ROOT / "recovery_vs_downtime.png")
COLORS = {"replog": "#4878a8", "redpanda": "#e49444", "kafka": "#59935b"}

pts = defaultdict(list)
for tdir in sorted(p for p in ROOT.iterdir() if p.is_dir()):
    t = tdir.name
    if t not in COLORS:
        continue
    for fc in tdir.glob("**/faults.csv"):
        for r in csv.DictReader(fc.open()):
            if r["role"] == "leader" and r.get("max_gap_ms") and r.get("heal_after_ms"):
                # kill and isolate: the broker is gone for heal_after; pause too.
                pts[t].append((int(r["heal_after_ms"]) / 1000, int(r["max_gap_ms"]) / 1000, r["fault"]))

fig, ax = plt.subplots(figsize=(8.5, 6.5))
lim = 0
for t, ps in pts.items():
    xs = [p[0] for p in ps]
    ys = [p[1] for p in ps]
    lim = max(lim, max(xs + ys, default=0))
    ax.scatter(xs, ys, s=42, color=COLORS[t], alpha=0.8, edgecolor="white", linewidth=0.6, label=f"{t} (n={len(ps)})")
lim = lim * 1.1 + 1
ax.plot([0, lim], [0, lim], "--", color="#888", linewidth=1, label="y = x  (recovers only when broker rejoins)")
ax.set_xlim(0, lim)
ax.set_ylim(0, lim)
ax.set_xlabel("broker downtime after a leader fault (s)")
ax.set_ylabel("acks=all recovery: largest ack gap in the fault window (s)")
ax.set_title("Recovery vs. downtime, identical seeds & containers, 1000 ms detection\n"
             "replog tracks y=x+~3 (waits for rejoin); Redpanda stays flat (~4–6 s, recovers on election)")
ax.grid(alpha=0.3)
ax.legend(loc="upper left")
fig.tight_layout()
fig.savefig(OUT, dpi=140)
print(f"wrote {OUT}")
for t, ps in pts.items():
    lo_heal = [p[1] for p in ps if p[0] < 8]
    hi_heal = [p[1] for p in ps if p[0] >= 8]
    def med(v): 
        v=sorted(v); return v[len(v)//2] if v else float("nan")
    print(f"{t}: heal<8s -> gap p50 {med(lo_heal):.1f}s (n={len(lo_heal)}); heal>=8s -> gap p50 {med(hi_heal):.1f}s (n={len(hi_heal)})")
