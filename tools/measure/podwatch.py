"""Sample the Knative function pods every few seconds during a run.

Output: pods.csv in the run directory, one row per revision and sample:
t (epoch s), service, revision, ready, not_ready, terminating.
"""
import argparse
import csv
import json
import os
import subprocess
import time
from collections import defaultdict


def sample(namespace):
    """Count the pods of each Knative revision by state."""
    out = subprocess.run(["kubectl", "get", "pods", "-n", namespace, "-o", "json"],
                         capture_output=True, text=True, check=True).stdout
    counts = defaultdict(lambda: {"ready": 0, "not_ready": 0, "terminating": 0})
    for pod in json.loads(out)["items"]:
        labels = pod["metadata"].get("labels", {})
        service = labels.get("serving.knative.dev/service")
        if not service:
            continue  # not a Knative function pod
        key = (service, labels.get("serving.knative.dev/revision", ""))
        statuses = pod.get("status", {}).get("containerStatuses", [])
        if pod["metadata"].get("deletionTimestamp"):
            counts[key]["terminating"] += 1
        elif statuses and all(s.get("ready") for s in statuses):
            counts[key]["ready"] += 1
        else:
            counts[key]["not_ready"] += 1
    return counts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, help="run directory")
    p.add_argument("--namespace", default="application")
    p.add_argument("--interval", type=float, default=2.0)
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "pods.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["t", "service", "revision", "ready", "not_ready", "terminating"])
        try:
            while True:  # runs until stopped (Ctrl+C or kill)
                t = time.time()
                try:
                    for (service, revision), c in sorted(sample(args.namespace).items()):
                        writer.writerow([round(t, 1), service, revision,
                                         c["ready"], c["not_ready"], c["terminating"]])
                    f.flush()
                except subprocess.CalledProcessError as e:
                    print(f"kubectl failed, sample skipped: {e}")
                time.sleep(max(0.0, args.interval - (time.time() - t)))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()