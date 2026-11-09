"""Per-bin request timeline of a measurement run: sent, failed (non-200), p95 and max latency of the successful requests."""
import csv
import sys
from collections import Counter, defaultdict

run_dir = sys.argv[1]
bin_s = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0
fault_s = float(sys.argv[3]) if len(sys.argv) > 3 else None

rows = list(csv.DictReader(open(f"{run_dir}/loadgen.csv")))
t0 = min(float(r["t_send"]) for r in rows)
bins = defaultdict(list)
for r in rows:
    bins[int((float(r["t_send"]) - t0) // bin_s)].append(r)

print("statuses:", dict(Counter(r["status"] for r in rows)))
print(f"{'t(s)':>8} {'sent':>5} {'fail':>5} {'p95ms':>6} {'maxms':>6}")
for b in sorted(bins):
    rs = bins[b]
    ok = sorted((float(r["t_done"]) - float(r["t_send"])) * 1000 for r in rs if r["status"] == "200")
    fail = sum(1 for r in rs if r["status"] != "200")
    p95 = ok[int(0.95 * (len(ok) - 1))] if ok else 0
    mx = ok[-1] if ok else 0
    mark = " <- fault" if fault_s is not None and b * bin_s <= fault_s < (b + 1) * bin_s else ""
    print(f"{b * bin_s:8.0f} {len(rs):5d} {fail:5d} {p95:6.0f} {mx:6.0f}{mark}")
