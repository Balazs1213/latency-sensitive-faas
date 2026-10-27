"""Split each layout switch into detection, execution and recovery phases.

Inputs: controller.log ([RECONFIG_PHASE] lines) and traces.csv (from collect_traces.py) in the run directory.
"""
import argparse
import csv
import json
import os

import numpy as np

WINDOW_S = 15  # same window as the controller's P95 query


def read_phases(path):
    """Parse the [RECONFIG_PHASE] JSON lines of the controller log."""
    events = []
    for line in open(path):
        if "[RECONFIG_PHASE] " in line:
            e = json.loads(line.split("[RECONFIG_PHASE] ", 1)[1])
            e["t"] = e["t_ms"] / 1000
            events.append(e)
    return sorted(events, key=lambda e: e["t"])


def read_traces(path):
    """Trace start/end times (s, epoch), latency (ms) and deployments."""
    rows = list(csv.DictReader(open(path)))
    start = np.array([int(r["start_us"]) / 1e6 for r in rows])
    end = np.array([int(r["end_us"]) / 1e6 for r in rows])
    lat = np.array([float(r["e2e_ms"]) for r in rows])
    deps = [r["deployments"] for r in rows]
    return start, end, lat, deps


def rolling_p95(times, lat):
    """P95 of traces whose time (end or start) fell in the last WINDOW_S seconds, evaluated every second."""
    grid = np.arange(times.min() + WINDOW_S, times.max() + 1, 1.0)
    values = np.array([np.percentile(lat[(times > g - WINDOW_S) & (times <= g)], 95)
                       if np.any((times > g - WINDOW_S) & (times <= g)) else np.nan for g in grid])
    return grid, values


def last_violation(grid, p95, prev, t_dec, limit):
    """Start of the last above-limit period before the decision."""
    above = (grid > prev) & (grid <= t_dec) & (p95 > limit)
    below = (grid > prev) & (grid <= t_dec) & (p95 <= limit)
    if not above.any():
        return None
    last_ok = grid[below].max() if below.any() else prev
    later = above & (grid > last_ok)
    return grid[later].min() if later.any() else None


def first(events, phase, after, before=float("inf")):
    """Time of the first event of a phase in (after, before]."""
    times = [e["t"] for e in events if e["phase"] == phase and after <= e["t"] <= before]
    return times[0] if times else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", required=True)
    p.add_argument("--limit", type=float, required=True, help="latency limit in ms")
    args = p.parse_args()

    events = read_phases(os.path.join(args.run, "controller.log"))
    start, end, lat, deps = read_traces(os.path.join(args.run, "traces.csv"))
    grid, p95 = rolling_p95(end, lat)
    grid_s, p95_s = rolling_p95(start, lat)  # by request start: when the problem really began
    t0 = start.min()
    decisions = [e for e in events if e["phase"] == "decision"]
    results = []

    for i, d in enumerate(decisions):
        t_dec = d["t"]
        prev = decisions[i - 1]["t"] if i > 0 else t0
        nxt = decisions[i + 1]["t"] if i + 1 < len(decisions) else float("inf")
        r = {"layout": d["layout"], "decision": t_dec}

        if d["layout"] == "max_rate":  # upgrade: there was a violation before the decision
            r["viol"] = last_violation(grid, p95, prev, t_dec, args.limit)
            r["viol_start"] = last_violation(grid_s, p95_s, prev, t_dec, args.limit)
            r["seen"] = first(events, "violation_seen", r["viol"] or prev, t_dec)

        ready = [e["t"] for e in events if e["phase"] in ("deployment_ready", "deployment_reused")
                 and t_dec <= e["t"] < nxt]
        r["ready"] = max(ready) if ready else None
        dns = [e for e in events if e["phase"] == "dns_updated" and t_dec <= e["t"] < nxt]
        if dns:
            r["dns"] = dns[0]["t"]
            served = start[[dns[0]["deployment_id"] in x for x in deps] & (start >= t_dec)]
            r["served"] = served.min() if served.size else None
            after = (grid > r["dns"]) & (p95 <= args.limit)
            r["recover"] = grid[after].min() if after.any() else None
        results.append(r)

    def span(r, a, b):
        return round(r[b] - r[a], 1) if r.get(a) and r.get(b) else None

    print(f"run start: {t0:.1f} (all times in s from run start)")
    for r in results:
        print(f"\n== decision -> {r['layout']} at {r['decision'] - t0:.1f} s")
        rows = [("detection (viol -> decision)", "viol", "decision"),
                ("  data lag (viol -> seen)", "viol", "seen"),
                ("  gates (seen -> decision)", "seen", "decision"),
                ("execution (decision -> served)", "decision", "served"),
                ("  pods ready (decision -> ready)", "decision", "ready"),
                ("  rerouting (ready -> dns)", "ready", "dns"),
                ("  propagation (dns -> served)", "dns", "served"),
                ("recovery (served -> P95 < limit)", "served", "recover"),
                ("above limit total (viol -> recover)", "viol", "recover"),
                ("detection by request start", "viol_start", "decision"),
                ("above limit by request start", "viol_start", "recover")]
        for name, a, b in rows:
            print(f"  {name:38s} {span(r, a, b)}")

    with open(os.path.join(args.run, "breakdown.json"), "w") as f:
        json.dump(results, f, indent=1, default=float)


if __name__ == "__main__":
    main()