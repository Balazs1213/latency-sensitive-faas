"""Collect the summary.csv of every run under a folder into one table (all-runs.csv)."""
import argparse
import csv
import glob
import os


def main():
    p = argparse.ArgumentParser()
    p.add_argument("root", help="folder searched recursively for run folders with a summary.csv")
    p.add_argument("--out", help="output CSV (default: <root>/all-runs.csv)")
    args = p.parse_args()
    root = os.path.expanduser(args.root)
    out = args.out or os.path.join(root, "all-runs.csv")

    rows = []
    for path in sorted(glob.glob(os.path.join(root, "**", "summary.csv"), recursive=True)):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                # The run's path below the root, e.g. 2026-11-06/check-multi-downgrade
                row["run"] = os.path.relpath(os.path.dirname(path), root)
                rows.append(row)

    # Union of the columns, in the order they first appear (older runs may lack some)
    cols = []
    for row in rows:
        cols += [k for k in row if k not in cols]
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} runs -> {out}")


if __name__ == "__main__":
    main()