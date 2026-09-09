# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib"]
# ///
"""Stage 6: side-by-side fault-injection results across targets.

Usage: uv run bench/plot_faults.py [results_root] [out.png]
       (default root bench/results/faults; targets = its subdirectories)

Reads every seed-*/faults.csv, seed-*/summary.txt, seed-*/schedule.log and
seed-*/timeline.csv under each target, prints a Markdown summary table to
stdout (also written to <root>/summary.md), and draws:
  - ECDFs of "leader moved" (t1) and "first ack after fault" (t2) per fault,
    leader-role partitions only, one line per target
  - ECDF of follower-fault stalls (first ack after a *follower* fault)
  - the acked/s timeline of seed 1 for each target with fault markers
"""

import csv
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results/faults")
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else ROOT / "faults.png")
COLORS = {"replog": "#4878a8", "redpanda": "#e49444", "kafka": "#59935b"}
FAULTS = ("kill", "pause", "isolate")


def pct(xs, q):
    xs = sorted(xs)
    return xs[round((len(xs) - 1) * q)] if xs else None


def fmt_p(xs):
    if not xs:
        return "–"
    return f"{pct(xs, 0.5)} / {pct(xs, 0.9)} / {max(xs)} (n={len(xs)})"


def parse_summary(path):
    s = path.read_text() if path.exists() else ""
    out = {"acked": 0, "missing": 0, "extra_dups": 0, "zero_secs": 0, "load_secs": 0, "violations": 0}
    m = re.search(r"checker: (\d+) acked", s)
    if m:
        out["acked"] = int(m.group(1))
    m = re.search(r"VIOLATED — (\d+) acked ids never consumed", s)
    if m:
        out["missing"] = int(m.group(1))
    out["violations"] = s.count("VIOLATED")
    m = re.search(r"availability: (\d+) of (\d+) load-seconds", s)
    if m:
        out["zero_secs"], out["load_secs"] = int(m.group(1)), int(m.group(2))
    m = re.search(r"duplicates: (\d+) deliveries beyond", s)
    if m:
        out["extra_dups"] = int(m.group(1))
    return out


targets = sorted(p.name for p in ROOT.iterdir() if p.is_dir() and any(p.glob("seed-*")))
per_target = {}
for t in targets:
    agg = {"moved": defaultdict(list), "ack_leader": defaultdict(list), "ack_follower": defaultdict(list),
           "faults": 0, "seeds": [], "summary": defaultdict(int), "controls": {}}
    for seed_dir in sorted((ROOT / t).glob("seed-*")):
        agg["seeds"].append(seed_dir.name)
        for k, v in parse_summary(seed_dir / "summary.txt").items():
            agg["summary"][k] += v
        fcsv = seed_dir / "faults.csv"
        if fcsv.exists():
            seen = set()
            for r in csv.DictReader(fcsv.open()):
                key = (r["t0_s"], r["victim"])
                if key not in seen:
                    seen.add(key)
                    agg["faults"] += 1
                if r["role"] == "leader":
                    if r["leader_moved_ms"]:
                        agg["moved"][r["fault"]].append(int(r["leader_moved_ms"]))
                    if r["first_ack_ms"]:
                        agg["ack_leader"][r["fault"]].append(int(r["first_ack_ms"]))
                elif r["first_ack_ms"]:
                    agg["ack_follower"][r["fault"]].append(int(r["first_ack_ms"]))
    for ctl in ("control-acks1", "control-acksall", "baseline"):
        d = ROOT / t / ctl
        if d.exists():
            agg["controls"][ctl] = parse_summary(d / "summary.txt")
    per_target[t] = agg

lines = []
lines.append("| target | seeds | faults | acked ids | contract violations | dups beyond 1/reader | zero-ack s / load s |")
lines.append("|---|---|---|---|---|---|---|")
for t, a in per_target.items():
    s = a["summary"]
    lines.append(f"| {t} | {len(a['seeds'])} | {a['faults']} | {s['acked']} | {s['violations']} "
                 f"| {s['extra_dups']} | {s['zero_secs']} / {s['load_secs']} |")
lines.append("")
lines.append("Leader faults — p50 / p90 / max in ms (metadata-visible leader change; first ack after fault):")
lines.append("")
lines.append("| target | fault | leader moved | first ack |")
lines.append("|---|---|---|---|")
for t, a in per_target.items():
    for f in FAULTS:
        if a["moved"][f] or a["ack_leader"][f]:
            lines.append(f"| {t} | {f} | {fmt_p(a['moved'][f])} | {fmt_p(a['ack_leader'][f])} |")
lines.append("")
lines.append("Follower faults — first ack after fault (the stall a follower's death costs a leader):")
lines.append("")
lines.append("| target | fault | first ack |")
lines.append("|---|---|---|")
for t, a in per_target.items():
    for f in FAULTS:
        if a["ack_follower"][f]:
            lines.append(f"| {t} | {f} | {fmt_p(a['ack_follower'][f])} |")
lines.append("")
lines.append("Controls (same seed, isolate-only schedule; only the ack level differs):")
lines.append("")
lines.append("| target | run | acked | acked ids lost | violations |")
lines.append("|---|---|---|---|---|")
for t, a in per_target.items():
    for ctl, s in a["controls"].items():
        lines.append(f"| {t} | {ctl} | {s['acked']} | {s['missing']} | {s['violations']} |")
table = "\n".join(lines)
print(table)
(ROOT / "summary.md").write_text(table + "\n")

# ------------------------------------------------------------------ figure
fig, axes = plt.subplots(2, 3, figsize=(15, 8.5))


def ecdf(ax, xs, label, color):
    xs = sorted(xs)
    if not xs:
        return
    ys = [(i + 1) / len(xs) for i in range(len(xs))]
    ax.step([0] + xs, [0] + ys, where="post", label=f"{label} (n={len(xs)})", color=color, linewidth=1.8)


for col, f in enumerate(FAULTS):
    ax = axes[0][col]
    for t, a in per_target.items():
        ecdf(ax, [x / 1000 for x in a["moved"][f]], f"{t} leader moved", COLORS.get(t, "#777"))
        ecdf(ax, [x / 1000 for x in a["ack_leader"][f]], f"{t} first ack", COLORS.get(t, "#777"))
        # dash the first-ack line
        if ax.lines:
            ax.lines[-1].set_linestyle("--")
    ax.set_title(f"leader {f}: time to new leader (solid) and to first ack (dashed)")
    ax.set_xlabel("seconds after fault")
    ax.set_ylabel("fraction of leader faults")
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)

ax = axes[1][0]
for t, a in per_target.items():
    allf = [x / 1000 for f in FAULTS for x in a["ack_follower"][f]]
    ecdf(ax, allf, f"{t} follower fault → first ack", COLORS.get(t, "#777"))
ax.set_title("follower faults: stall until the next ack (all fault kinds)")
ax.set_xlabel("seconds after fault")
ax.set_ylabel("fraction of follower faults")
ax.grid(alpha=0.3)
ax.legend(fontsize=8)

for i, t in enumerate(list(per_target)[:2]):
    ax = axes[1][1 + i]
    seed_dir = ROOT / t / "seed-1"
    tl = seed_dir / "timeline.csv"
    if tl.exists():
        per_sec = defaultdict(int)
        for r in csv.DictReader(tl.open()):
            per_sec[int(r["second"])] += int(r["acked"])
        xs = sorted(per_sec)
        ax.plot(xs, [per_sec[x] for x in xs], color=COLORS.get(t, "#777"), linewidth=1.2)
        sched = seed_dir / "schedule.log"
        if sched.exists():
            for line in sched.read_text().splitlines():
                m = re.match(r"\[t=\s*([\d.]+)s\] FAULT (\w+) broker (\d+) \(([^)]*)\) \[(.*?)\]", line)
                if m:
                    t0 = float(m.group(1))
                    is_leader = "leader" in m.group(5)
                    ax.axvline(t0, color="#c0392b" if is_leader else "#999", alpha=0.7 if is_leader else 0.4,
                               linewidth=1)
                    ax.text(t0, ax.get_ylim()[1] * 0.98, m.group(2)[0], fontsize=7, ha="center", va="top",
                            color="#c0392b" if is_leader else "#666")
    ax.set_title(f"{t}, seed 1: acked ids per second (red = leader fault, grey = follower; k/p/i)")
    ax.set_xlabel("seconds")
    ax.set_ylabel("acked / s")
    ax.grid(alpha=0.3)

fig.suptitle("Identical seeded fault schedules, identical containers, 1000 ms failure-detection timeout", fontsize=12)
fig.tight_layout()
fig.savefig(OUT, dpi=130)
print(f"wrote {OUT}")
