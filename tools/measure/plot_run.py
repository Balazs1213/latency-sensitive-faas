"""Plot one measurement run in the style of Egon's figures 21-22."""
import argparse
import csv
import os
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # no display on the VM, write PNG only
import matplotlib.pyplot as plt
import numpy as np


def to_us(iso):
    """Convert an ISO UTC time (e.g. 2026-10-19T12:13:51Z) to epoch microseconds."""
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1e6)


def rolling_p95(t, lat, window_s=15):
    """P95 of the traces started in the last window_s seconds, evaluated every second."""
    grid = np.arange(window_s, t.max(), 1.0)
    values = []
    for g in grid:
        mask = (t > g - window_s) & (t <= g)
        values.append(np.percentile(lat[mask], 95) if mask.any() else np.nan)
    return grid, values


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", required=True, help="run directory with traces.csv")
    p.add_argument("--start", required=True, help="UTC time used as t=0")
    p.add_argument("--limit", type=float, help="latency limit in ms (optional)")
    p.add_argument("--switch", action="append", default=[],
                    help="switch start time and duration, e.g. 2026-10-19T12:13:51Z,32.7")
    p.add_argument("--title", default="End-to-end latency vs RPS")
    args = p.parse_args()

    t0 = to_us(args.start)
    rows = list(csv.DictReader(open(os.path.join(args.run, "traces.csv"))))
    t = np.array([(int(r["start_us"]) - t0) / 1e6 for r in rows])
    lat = np.array([float(r["e2e_ms"]) for r in rows])
    keep = t >= 0
    t, lat = t[keep], lat[keep]

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.scatter(t, lat, s=4, color="tab:red", alpha=0.6, label="Latency per trace (ms)")
    grid, p95 = rolling_p95(t, lat)
    ax.plot(grid, p95, color="black", linewidth=1, label="Rolling P95 (15 s)")
    if args.limit:
        ax.axhline(args.limit, color="black", linestyle="--",
                   label=f"Latency limit ({args.limit:.0f} ms)")

    top = ax.get_ylim()[1]
    for i, sw in enumerate(args.switch):
        start_iso, dur = sw.split(",")
        dur = float(dur)
        begin = (to_us(start_iso) - t0) / 1e6
        ax.axvspan(begin, begin + dur, color="red", alpha=0.15,
                   label="Reconfiguration" if i == 0 else None)
        ax.text(begin + dur / 2, top * 0.95, f"{dur:.1f} s", color="red", ha="center")

    ax.set_xlabel("Time (seconds from start)")
    ax.set_ylabel("Latency (ms)", color="tab:red")

    # Traces started per second, averaged over 30 s windows (Egon: RPS, 30 s avg)
    ax2 = ax.twinx()
    edges = np.arange(0, t.max() + 30, 30)
    counts, _ = np.histogram(t, bins=edges)
    ax2.plot(edges[:-1] + 15, counts / 30, color="tab:blue", label="RPS (30 s avg)")
    ax2.set_ylabel("RPS (avg per 30 s)", color="tab:blue")
    ax2.set_ylim(bottom=0)

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8)
    ax.set_title(args.title)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    out = os.path.join(args.run, "run.png")
    fig.savefig(out, dpi=150)
    print("saved", out)


if __name__ == "__main__":
    main()