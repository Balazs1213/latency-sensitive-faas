"""Per-bin timeline of a measurement run around an injected fault.

HTTP columns (loadgen.csv): requests sent in the bin, how many of them failed (non-200), and the longest wait (s) of a
request in the load generator between its scheduled and its actual send time. The HTTP response only
acknowledges the request; the processing continues asynchronously, so the latency comes from the traces.
Trace columns (traces.csv): complete traces that started in the bin, their p95 and maximum end-to-end latency, how
many exceeded the latency limit, and how many traces started in the bin but never completed.

Usage: fault_timeline.py RUN_DIR [BIN_S] [FAULT_S] [LIMIT_MS]
"""
import csv
import sys
from collections import Counter, defaultdict

run_dir = sys.argv[1]
bin_s = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0
fault_s = float(sys.argv[3]) if len(sys.argv) > 3 else None
limit_ms = float(sys.argv[4]) if len(sys.argv) > 4 else 1000.0

requests = list(csv.DictReader(open(f"{run_dir}/loadgen.csv")))
t0 = min(float(r["t_send"]) for r in requests)
sent = defaultdict(int)
failed = defaultdict(int)
wait = defaultdict(float)
for r in requests:
    b = int((float(r["t_send"]) - t0) // bin_s)
    sent[b] += 1
    failed[b] += r["status"] != "200"
    wait[b] = max(wait[b], float(r["t_send"]) - float(r["t_sched"]))

latencies = defaultdict(list)
incomplete = defaultdict(int)
for t in csv.DictReader(open(f"{run_dir}/traces.csv")):
    b = int((int(t["start_us"]) / 1e6 - t0) // bin_s)
    if t["has_start"] == "True" and t["has_end"] == "True":
        latencies[b].append(float(t["e2e_ms"]))
    else:
        incomplete[b] += 1

print("HTTP statuses:", dict(Counter(r["status"] for r in requests)))
print(f"{'t(s)':>6} {'sent':>5} {'fail':>5} {'waits':>6} | {'traces':>6} {'p95ms':>6} {'maxms':>6} {'late':>5} {'incompl':>7}")
for b in sorted(set(sent) | set(latencies) | set(incomplete)):
    lat = sorted(latencies[b])
    p95 = lat[int(0.95 * (len(lat) - 1))] if lat else 0
    mx = lat[-1] if lat else 0
    late = sum(1 for x in lat if x > limit_ms)
    mark = " <- fault" if fault_s is not None and b * bin_s <= fault_s < (b + 1) * bin_s else ""
    print(f"{b * bin_s:6.0f} {sent[b]:5d} {failed[b]:5d} {wait[b]:6.1f} | {len(lat):6d} {p95:6.0f} {mx:6.0f} {late:5d} {incomplete[b]:7d}{mark}")
