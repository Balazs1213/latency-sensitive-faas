"""Processing time of the components vs. everything else, over the run in time bins.

Shows whether requests get slower under load because the components themselves run slower
(e.g. CPU contention) or because the requests wait longer before and between them (queueing).
Input: traces.csv of a run (from collect_traces.py).
"""
import argparse
import csv
import os

import numpy as np

COMPONENTS = ["detect-track-service_ms", "position-service_ms", "ru-fusion-service_ms"]


def pct(values, q):
    return round(float(np.percentile(values, q)))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", required=True, help="run directory with traces.csv")
    p.add_argument("--bin", type=float, default=30.0, help="bin width in seconds")
    args = p.parse_args()

    rows = [r for r in csv.DictReader(open(os.path.join(args.run, "traces.csv")))
            if r["has_start"] == "True" and r["has_end"] == "True"]
    t0 = min(int(r["start_us"]) for r in rows) / 1e6

    bins = {}
    for r in rows:
        b = int((int(r["start_us"]) / 1e6 - t0) // args.bin)
        e2e = float(r["e2e_ms"])
        work = sum(float(r.get(c) or 0) for c in COMPONENTS)  # time spent inside the components
        split = len(r["deployments"].split("|")) > 1          # went through more than one function
        bins.setdefault(b, []).append((e2e, float(r.get(COMPONENTS[0]) or 0), e2e - work, split))

    print(f"{'from s':>6} {'req/s':>5} {'split':>5} | {'e2e p50':>7} {'p95':>5} | "
          f"{'detect p50':>10} {'p95':>5} | {'other p50':>9} {'p95':>5}")
    for b in sorted(bins):
        v = np.array(bins[b], dtype=float)
        print(f"{b * args.bin:6.0f} {len(v) / args.bin:5.1f} {v[:, 3].mean():5.0%} | "
              f"{pct(v[:, 0], 50):7d} {pct(v[:, 0], 95):5d} | "
              f"{pct(v[:, 1], 50):10d} {pct(v[:, 1], 95):5d} | "
              f"{pct(v[:, 2], 50):9d} {pct(v[:, 2], 95):5d}")


if __name__ == "__main__":
    main()