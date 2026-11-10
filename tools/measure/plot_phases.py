"""Overview and per-phase plots of one run, plus a per-phase statistics table.

Writes into the run directory:
  phases.csv                per phase: requests, late requests, percentiles, time above the limit, pods, switches
  phases.txt                the main columns of phases.csv as a readable table
  plots/00-overview/        the whole run, phases shaded
  plots/00-all-phases/      all phases side by side, each zoomed to its own time range
  plots/NN-<phase>/         one phase, zoomed
Every plot folder holds combined.png (latency, offered vs. completed rate and ready pods in three
rows) and the same rows as separate files: latency.png, rate.png, pods.png.

Usage:
  python tools/measure/plot_phases.py --run DIR --limit 1000 \
      --phases "warm-up:0-60,staircase up:60-420,staircase down:420-540,quiet 1:540-900,jump:900-1080,quiet 2:1080-1440"
Without --phases, --profile (the loadgen profile) gives one phase per profile segment.
Time 0 is the start of the load generator.
"""
import argparse
import csv
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")  # no display on the VM, write PNG only
import matplotlib.pyplot as plt
import numpy as np
from reconfig_breakdown import read_phases

P95_WINDOW_S = 15


def parse_phases(text):
    """'name:start-end,...' -> [(name, start, end)]"""
    phases = []
    for part in text.split(","):
        name, _, span = part.rpartition(":")
        start, end = span.split("-")
        phases.append((name.strip(), float(start), float(end)))
    return phases


def phases_from_profile(text):
    """One phase per loadgen profile segment, named after its rate."""
    phases, offset = [], 0.0
    for part in text.split(","):
        dur, rates = part.split(":")
        phases.append((f"{rates}/s", offset, offset + float(dur)))
        offset += float(dur)
    return phases


def read_run(run):
    """Load generator rows, traces, pod samples and switch bands, all in seconds from the load start."""
    lg = list(csv.DictReader(open(os.path.join(run, "loadgen.csv"))))
    t0 = min(float(r["t_sched"]) for r in lg)
    sched = np.array([float(r["t_sched"]) - t0 for r in lg])
    failed = np.array([r["status"] != "200" for r in lg])

    tr = list(csv.DictReader(open(os.path.join(run, "traces.csv"))))
    start = np.array([int(r["start_us"]) / 1e6 - t0 for r in tr])
    end = np.array([int(r["end_us"]) / 1e6 - t0 for r in tr])
    lat = np.array([float(r["e2e_ms"]) for r in tr])
    order = np.argsort(start)
    start, end, lat = start[order], end[order], lat[order]

    # Ready pods per Knative service (summed over its revisions) at every sample time
    pods = defaultdict(dict)
    pods_path = os.path.join(run, "pods.csv")
    if os.path.exists(pods_path):
        for r in csv.DictReader(open(pods_path)):
            if not r["service"].startswith("d-"):
                continue  # other apps in the namespace (e.g. the old func-N demo) are not part of the run
            t = float(r["t"]) - t0
            pods[r["service"]][t] = pods[r["service"]].get(t, 0) + int(r["ready"])
    times = sorted({t for s in pods.values() for t in s})
    pod_t = np.array(times)
    pod_series = {s: np.array([v.get(t, 0) for t in times]) for s, v in pods.items()}

    bands = []
    log_path = os.path.join(run, "controller.log")
    if os.path.exists(log_path):
        events = read_phases(log_path)
        for i, e in enumerate(events):
            if e["phase"] != "decision":
                continue
            later = [x for x in events[i + 1:] if x["phase"] in ("decision", "dns_updated")]
            if later and later[0]["phase"] == "dns_updated":
                bands.append((e["t"] - t0, later[0]["t"] - e["t"], e["layout"]))
    return dict(sched=sched, failed=failed, start=start, end=end, lat=lat,
                pod_t=pod_t, pod_series=pod_series, bands=bands, duration=sched.max())


def rolling_p95(start, lat, lo, hi):
    """P95 of the traces started in the last P95_WINDOW_S seconds, every second in [lo, hi]."""
    grid = np.arange(max(lo, P95_WINDOW_S), hi + 1, 1.0)
    i0 = np.searchsorted(start, grid - P95_WINDOW_S, side="right")
    i1 = np.searchsorted(start, grid, side="right")
    p95 = np.array([np.percentile(lat[a:b], 95) if b > a else np.nan for a, b in zip(i0, i1)])
    return grid, p95


def rate(times, lo, hi, bin_s):
    edges = np.arange(lo, hi + bin_s, bin_s)
    counts, _ = np.histogram(times, bins=edges)
    return edges[:-1] + bin_s / 2, counts / bin_s


def short(service):
    return service[:9]


def decorate(ax, d, lo, hi, phases):
    """Common x range, grid, alternating phase shading and switch lines."""
    ax.set_xlim(lo, hi)
    ax.grid(alpha=0.3)
    for i, (name, a, b) in enumerate(phases):
        if b > lo and a < hi and i % 2:
            ax.axvspan(max(a, lo), min(b, hi), color="grey", alpha=0.07)
    for begin, dur, layout in d["bands"]:
        if lo <= begin <= hi:
            ax.axvline(begin, color="red", lw=1, alpha=0.7)


def draw_latency(ax, d, lo, hi, limit, phases, show_names):
    m = (d["start"] >= lo) & (d["start"] <= hi)
    ax.scatter(d["start"][m], d["lat"][m], s=3, color="tab:red", alpha=0.5, label="latency per request")
    grid, p95 = rolling_p95(d["start"], d["lat"], lo, hi)
    ax.plot(grid, p95, color="black", lw=1, label=f"rolling P95 ({P95_WINDOW_S} s)")
    if limit:
        ax.axhline(limit, color="black", ls="--", lw=0.8, label="latency limit")
    # Every point and the P95 line of this window fit on the axis, with room for the labels on top
    peaks = [2.5 * limit] + ([float(d["lat"][m].max())] if m.any() else []) + \
        ([float(np.nanmax(p95))] if np.any(np.isfinite(p95)) else [])
    ytop = max(peaks) * 1.12
    ax.set_ylim(0, ytop)
    decorate(ax, d, lo, hi, phases)
    for begin, dur, layout in d["bands"]:
        if lo <= begin <= hi:
            arrow = {"max_rate": "↑", "min_rate": "↓"}.get(layout, "")
            ax.text(begin, ytop * 0.93, f"{arrow}{dur:.1f}s", color="red", fontsize=7, ha="center")
    if show_names:
        for name, a, b in phases:
            if b > lo and a < hi:
                ax.text((max(a, lo) + min(b, hi)) / 2, ytop * 1.02, name, ha="center",
                        va="bottom", fontsize=8)


def draw_rate(ax, d, lo, hi, phases, bin_s):
    x, offered = rate(d["sched"], lo, hi, bin_s)
    ax.step(x, offered, where="mid", color="tab:blue", label="offered (sent)")
    x, done = rate(d["end"], lo, hi, bin_s)
    ax.step(x, done, where="mid", color="tab:green", label="completed")
    ax.set_ylim(bottom=0)
    decorate(ax, d, lo, hi, phases)


def draw_pods(ax, d, lo, hi, phases):
    if len(d["pod_t"]):
        total = np.zeros_like(d["pod_t"], dtype=float)
        for s, v in sorted(d["pod_series"].items()):
            ax.step(d["pod_t"], v, where="post", lw=1, label=short(s))
            total += v
        ax.step(d["pod_t"], total, where="post", color="black", lw=1.2, label="total")
    ax.set_ylim(bottom=0)
    decorate(ax, d, lo, hi, phases)


ROWS = ("latency", "rate", "pods")
YLABEL = {"latency": "latency (ms)", "rate": "requests/s", "pods": "ready pods"}


def draw_row(row, ax, d, lo, hi, limit, phases, bin_s, show_names):
    if row == "latency":
        draw_latency(ax, d, lo, hi, limit, phases, show_names)
    elif row == "rate":
        draw_rate(ax, d, lo, hi, phases, bin_s)
    else:
        draw_pods(ax, d, lo, hi, phases)


def save(fig, folder, name):
    os.makedirs(folder, exist_ok=True)
    fig.tight_layout()
    fig.savefig(os.path.join(folder, name), dpi=130)
    plt.close(fig)


def plot_window(folder, title, d, lo, hi, limit, phases, bin_s, show_names):
    """combined.png (three rows) and latency.png, rate.png, pods.png for one time window."""
    width = min(18, max(9, (hi - lo) / 60))
    fig, axes = plt.subplots(3, 1, figsize=(width, 9), sharex=True,
                             gridspec_kw={"height_ratios": [3, 1.3, 1.3]})
    for row, ax in zip(ROWS, axes):
        draw_row(row, ax, d, lo, hi, limit, phases, bin_s, show_names)
        ax.set_ylabel(YLABEL[row])
        ax.legend(loc="best", fontsize=7)
    axes[-1].set_xlabel("time from the load start (s)")
    fig.suptitle(title)
    save(fig, folder, "combined.png")
    for row in ROWS:
        fig, ax = plt.subplots(figsize=(width, 4.5 if row == "latency" else 3.5))
        draw_row(row, ax, d, lo, hi, limit, phases, bin_s, show_names)
        ax.set_ylabel(YLABEL[row])
        ax.set_xlabel("time from the load start (s)")
        ax.legend(loc="best", fontsize=7)
        ax.set_title(f"{title}: {row}")
        save(fig, folder, f"{row}.png")


def phase_window(a, b):
    """Time range shown for a phase: a longer lead-in, so the change at the phase start is not at the edge"""
    return max(a - max(15.0, 0.3 * (b - a)), -5.0), b + max(5.0, 0.1 * (b - a))


def plot_side_by_side(folder, title, d, limit, phases):
    """All phases next to each other, each zoomed: combined.png and one file per row."""
    n = len(phases)
    windows = [(name, *phase_window(a, b), a, b) for name, a, b in phases]

    def grid(rows, height):
        fig, axes = plt.subplots(len(rows), n, figsize=(max(4 * n, 8), height), squeeze=False,
                                 gridspec_kw={"height_ratios": [3 if r == "latency" else 1.3 for r in rows]})
        for j, (name, lo, hi, a, b) in enumerate(windows):
            for i, row in enumerate(rows):
                ax = axes[i, j]
                draw_row(row, ax, d, lo, hi, limit, phases, 5, False)
                if j == 0:
                    ax.set_ylabel(YLABEL[row])
                    ax.legend(loc="best", fontsize=6)
            axes[0, j].set_title(f"{name}\n{a:.0f}–{b:.0f} s", fontsize=9)
            axes[-1, j].set_xlabel("s")
        return fig

    fig = grid(ROWS, 8)
    fig.suptitle(f"{title} – phases")
    save(fig, folder, "combined.png")
    for row in ROWS:
        fig = grid((row,), 3.5 if row == "latency" else 2.8)
        fig.suptitle(f"{title} – phases: {row}")
        save(fig, folder, f"{row}.png")


def slug(name):
    return "".join(c if c.isalnum() else "-" for c in name.lower()).strip("-")


def phase_stats(d, phases, limit):
    rows = []
    for name, a, b in phases:
        m = (d["start"] >= a) & (d["start"] < b)
        lat = d["lat"][m]
        sent = (d["sched"] >= a) & (d["sched"] < b)
        grid, p95 = rolling_p95(d["start"], d["lat"], a, b)
        row = {"phase": name, "start_s": a, "end_s": b,
               "sent": int(sent.sum()), "failed": int((sent & d["failed"]).sum()),
               "completed": int(m.sum())}
        if len(lat):
            row.update({"over_limit": int((lat > limit).sum()),
                        "within_limit_pct": round(100 * (lat <= limit).mean(), 2),
                        "p50_ms": round(np.percentile(lat, 50)), "p95_ms": round(np.percentile(lat, 95)),
                        "p99_ms": round(np.percentile(lat, 99)), "max_ms": round(lat.max())})
        row["p95_above_limit_s"] = int(np.nansum(p95 > limit))
        # Time-averaged ready pods (pod-seconds / duration) per service and in total
        pm = (d["pod_t"] >= a) & (d["pod_t"] < b)
        total = 0.0
        for s, v in sorted(d["pod_series"].items()):
            avg = float(v[pm].mean()) if pm.any() else 0.0
            row[f"pods_{short(s)}"] = round(avg, 2)
            total += avg
        row["pods_total"] = round(total, 2)
        row["switches_up"] = sum(1 for t, _, l in d["bands"] if a <= t < b and l == "max_rate")
        row["switches_down"] = sum(1 for t, _, l in d["bands"] if a <= t < b and l == "min_rate")
        rows.append(row)
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", required=True)
    p.add_argument("--limit", type=float, default=1000)
    p.add_argument("--phases", help="name:start-end,... in seconds from the load start")
    p.add_argument("--profile", help="loadgen profile; one phase per segment if --phases is missing")
    p.add_argument("--title", default="")
    args = p.parse_args()

    d = read_run(args.run)
    if args.phases:
        phases = parse_phases(args.phases)
    elif args.profile:
        phases = phases_from_profile(args.profile)
    else:
        phases = [("whole run", 0.0, float(np.ceil(d["duration"])))]
    end = max(b for _, _, b in phases) + 30  # the tail after the load ends

    title = args.title or os.path.basename(os.path.normpath(args.run))
    plots = os.path.join(args.run, "plots")
    plot_window(os.path.join(plots, "00-overview"), title, d, 0, end, args.limit, phases, 10, True)
    plot_side_by_side(os.path.join(plots, "00-all-phases"), title, d, args.limit, phases)
    for i, (name, a, b) in enumerate(phases, 1):
        lo, hi = phase_window(a, b)
        plot_window(os.path.join(plots, f"{i:02d}-{slug(name)}"), f"{title} – {name} ({a:.0f}–{b:.0f} s)",
                    d, lo, hi, args.limit, phases, 5, False)

    rows = phase_stats(d, phases, args.limit)
    cols = []
    for r in rows:
        cols += [k for k in r if k not in cols]
    with open(os.path.join(args.run, "phases.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    keys = ["phase", "completed", "over_limit", "within_limit_pct", "p95_ms", "max_ms",
            "p95_above_limit_s", "pods_total", "switches_up", "switches_down"]
    # The same table readable in the run directory (phases.txt) and on the screen
    lines = [" ".join(f"{k:>17}" if i else f"{k:<16}" for i, k in enumerate(keys))]
    for r in rows:
        lines.append(" ".join(f"{str(r.get(k, '')):>17}" if i else f"{str(r.get(k, '')):<16}"
                              for i, k in enumerate(keys)))
    with open(os.path.join(args.run, "phases.txt"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    print("saved phases.csv, phases.txt and the plots/ folder in", args.run)


if __name__ == "__main__":
    main()