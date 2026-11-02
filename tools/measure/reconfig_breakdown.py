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



def recovery_time(grid, p95, after, limit, hold=WINDOW_S):
    """Start of the first period after `after` in which the P95 stays at or below the limit for `hold` seconds."""
    run_start = None
    for g, v in zip(grid, p95):
        if g < after:
            continue
        if np.isnan(v) or v <= limit:  # no traces in the window counts as below the limit
            if run_start is None:
                run_start = g
            if g - run_start >= hold:
                return run_start
        else:
            run_start = None
    return None


def first_violation_after(grid, p95, a, b, limit):
    """First time in [a, b] when the P95 is above the limit."""
    m = (grid >= a) & (grid <= b) & (p95 > limit)
    return grid[m].min() if m.any() else None



def request_stats(path, limit):
    """Per-request statistics of the whole run, independent of the rolling window."""
    rows = list(csv.DictReader(open(path)))
    complete = [r for r in rows if r["has_start"] == "True" and r["has_end"] == "True"]
    lat = np.array([float(r["e2e_ms"]) for r in complete])
    over = lat > limit
    return {
        "requests": len(rows),
        "complete": len(complete),
        "over_limit": int(over.sum()),
        "within_limit_pct": round(100 * (1 - over.mean()), 2),
        "p50_ms": round(float(np.percentile(lat, 50))),
        "p95_ms": round(float(np.percentile(lat, 95))),
        "p99_ms": round(float(np.percentile(lat, 99))),
        "max_ms": round(float(lat.max())),
        "excess_s": round(float((lat[over] - limit).sum()) / 1000, 1),
    }



SWITCH_PRE_S = 30   # switch window: requests that started this long before an upgrade decision ...
SWITCH_POST_S = 60  # ... up to this long after it


def latency_stats(lat, limit):
    """Summary statistics of a set of request latencies (ms)."""
    if lat.size == 0:
        return {"requests": 0}
    over = lat > limit
    return {
        "requests": int(lat.size),
        "over_limit": int(over.sum()),
        "within_limit_pct": round(100 * (1 - over.mean()), 2),
        "p95_ms": round(float(np.percentile(lat, 95))),
        "max_ms": round(float(lat.max())),
        "excess_s": round(float((lat[over] - limit).sum()) / 1000, 1),
    }


def split_stats(path, limit, upgrades):
    """Request statistics around the upgrade decisions vs. the rest of the run (by request start)."""
    rows = [r for r in csv.DictReader(open(path)) if r["has_start"] == "True" and r["has_end"] == "True"]
    start = np.array([int(r["start_us"]) / 1e6 for r in rows])
    lat = np.array([float(r["e2e_ms"]) for r in rows])
    near = np.zeros(len(rows), dtype=bool)
    for t in upgrades:
        near |= (start >= t - SWITCH_PRE_S) & (start <= t + SWITCH_POST_S)
    return {"around_switch": latency_stats(lat[near], limit), "rest": latency_stats(lat[~near], limit)}

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
            # Recovered when the P95 stays below the limit for a whole window (a short dip does not count)
            r["recover"] = recovery_time(grid, p95, r["dns"], args.limit)
        # The violation may start only after the decision (e.g. the new layout is not ready yet)
        if d["layout"] == "max_rate" and r.get("recover"):
            if r.get("viol") is None:
                r["viol"] = first_violation_after(grid, p95, t_dec, r["recover"], args.limit)
            if r.get("viol_start") is None:
                r["viol_start"] = first_violation_after(grid_s, p95_s, t_dec, r["recover"], args.limit)
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

    # Per-request view of the whole run: independent of the rolling window
    stats = request_stats(os.path.join(args.run, "traces.csv"), args.limit)
    print("\n== requests (whole run)")
    for k, v in stats.items():
        print(f"  {k:38s} {v}")

    # The same requests split: around the upgrade decisions vs. the rest of the run
    upgrades = [r["decision"] for r in results if r["layout"] == "max_rate"]
    split = split_stats(os.path.join(args.run, "traces.csv"), args.limit, upgrades)
    for name, s in split.items():
        print(f"\n== requests {name.replace('_', ' ')} (window: -{SWITCH_PRE_S} s .. +{SWITCH_POST_S} s around upgrades)")
        for k, v in s.items():
            print(f"  {k:38s} {v}")

    with open(os.path.join(args.run, "breakdown.json"), "w") as f:
        json.dump({"switches": results, "requests": stats, "split": split}, f, indent=1, default=float)

if __name__ == "__main__":
    main()