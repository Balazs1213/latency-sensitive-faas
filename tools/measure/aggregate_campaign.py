"""Summary of the measurement campaigns over all nights: mean and standard deviation over the repetitions.

Reads the day folders of thesis-final (only runs with a DONE file) and writes into thesis-final/analysis/:
  runs.csv               one row per run (whole run): late requests, within the limit, p95/p99/max, mean ready pods
  normal_runs.txt        normal operation, whole runs: mean ± std per profile and configuration
  normal_phases.csv/.txt normal operation per phase: mean ± std per profile, phase and configuration
  normal_<profile>.png   per phase, the configurations side by side: within the limit, time above the limit, pods
  faults.txt/.csv        fault and overload experiments: request outcomes and latency, mean ± std
  nights.txt             which nights were used, with their image

Usage:
  python tools/measure/aggregate_campaign.py [--root ~/thesis/measurements/thesis-final] [--image dev-518e76d]
Only nights whose campaign finished are used (--all: also the runs of interrupted nights). --image keeps only the
nights measured with that framework image (from code.txt), e.g. to exclude nights before a code change. Run it any time; it always recomputes everything from the day folders.
"""
import argparse
import csv
import glob
import os
import re
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")  # no display on the VM, write PNG only
import matplotlib.pyplot as plt
import numpy as np

# Experiment folders of the nights before the current folder layout, under their current names: the nights before
# 2026-11-17 had no protection level, the pilots of 2026-11-17 had <without-backup|with-backup>/ under the group
LEGACY_EXPERIMENTS = {
    "2-faults/1-pod-loss-with-spare": "2-faults/1-pod-loss/without-spare-pods/1-detect-pod-of-many",
    "2-faults/2-pod-loss-single-replica": "2-faults/1-pod-loss/without-spare-pods/2-fusion-only-pod",
    "2-faults/3-node-loss-90s": "2-faults/2-node-unavailable/without-backup-layout/1-node-loss-90s",
    "3-overload/4-node-overload-simulated-90s":
        "2-faults/2-node-unavailable/without-backup-layout/2-node-overload-simulated-90s",
}


def experiment_name(parts):
    """Current name (<group>/<experiment path>) of the experiment of a run folder, given without repN"""
    backup = parts[1] if len(parts) > 2 and parts[1] in ("without-backup", "with-backup") else None
    if backup:
        parts = [parts[0]] + parts[2:]
    name = "/".join(parts)
    name = LEGACY_EXPERIMENTS.get(name, name)
    if backup == "with-backup":
        name = name.replace("/without-backup-layout/", "/with-backup-layout/")
    return name


CONFIGS = ["1-baseline-original", "2-full-prewarm", "3-proactive-early-switch", "4-follow-rate",
           "5-follow-rate-forecast"]
SHORT = {"1-baseline-original": "baseline", "2-full-prewarm": "full", "3-proactive-early-switch": "proactive",
         "4-follow-rate": "follow", "5-follow-rate-forecast": "follow+forecast"}
OUTCOMES = [("completed within the limit", "within"), ("completed above the limit", "above"),
            ("accepted (HTTP 200) but not completed", "lost"), ("rejected at the entry", "rejected"),
            ("HTTP 500", "http500")]


def night_image(day):
    try:
        for line in open(os.path.join(day, "code.txt")):
            if line.startswith("image:"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return ""


def night_complete(day):
    """The night's last full campaign (not a single-experiment ONLY run) finished without aborting"""
    try:
        lines = open(os.path.join(day, "campaign.log")).read().splitlines()
    except OSError:
        return False
    starts = [i for i, l in enumerate(lines) if "campaign start" in l and "only" not in l]
    if not starts:
        return False
    rest = lines[starts[-1]:]
    return any("campaign finished" in l for l in rest) and not any("aborting" in l for l in rest)


def whole_run(run):
    """Whole-run numbers: the first block of reconfig_breakdown in run_output.txt, mean pods from phases.csv"""
    out = {}
    text = open(os.path.join(run, "run_output.txt")).read()
    block = text.split("== requests (whole run)", 1)[-1]
    for key in ("complete", "over_limit", "within_limit_pct", "p50_ms", "p95_ms", "p99_ms", "max_ms"):
        m = re.search(rf"^\s+{key}\s+([\d.]+)", block, re.M)
        out[key] = float(m.group(1)) if m else np.nan
    phases = read_phases(run)
    dur = sum(p["end_s"] - p["start_s"] for p in phases)
    out["pods_mean"] = sum(p["pods_total"] * (p["end_s"] - p["start_s"]) for p in phases) / dur if dur else np.nan
    return out


def read_phases(run):
    rows = []
    for r in csv.DictReader(open(os.path.join(run, "phases.csv"))):
        rows.append({k: (r[k] if k == "phase" else float(r[k]) if r[k] not in ("", None) else np.nan)
                     for k in r})
    return rows


def outcomes(run):
    """Request outcomes from fault_timeline.txt, in % of the requests sent"""
    out = {}
    try:
        for line in open(os.path.join(run, "fault_timeline.txt")):
            for text, key in OUTCOMES:
                if line.strip().startswith(text):
                    out[key] = float(line.split()[-1].rstrip("%"))
    except OSError:
        pass
    return out


def ms(values, digits=1):
    """'mean ± std (n)' of the finite values"""
    v = np.array([x for x in values if x == x], dtype=float)
    if len(v) == 0:
        return "-"
    if len(v) == 1:
        return f"{v[0]:.{digits}f} (n=1)"
    return f"{v.mean():.{digits}f} ± {v.std(ddof=1):.{digits}f} (n={len(v)})"


def mean_std(values):
    v = np.array([x for x in values if x == x], dtype=float)
    if len(v) == 0:
        return np.nan, np.nan, 0
    return v.mean(), (v.std(ddof=1) if len(v) > 1 else 0.0), len(v)


def table(path, header, rows):
    widths = [max(len(str(x)) for x in col) for col in zip(header, *rows)]
    with open(path, "w") as f:
        for r in [header] + rows:
            f.write("  ".join(str(x).ljust(w) for x, w in zip(r, widths)).rstrip() + "\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=os.path.expanduser("~/thesis/measurements/thesis-final"))
    p.add_argument("--image", help="keep only nights measured with this image (substring of code.txt's image)")
    p.add_argument("--all", action="store_true", help="also use the runs of nights that did not finish")
    args = p.parse_args()
    out = os.path.join(args.root, "analysis")
    os.makedirs(out, exist_ok=True)

    days = sorted(d for d in glob.glob(os.path.join(args.root, "20??-??-??")) if os.path.isdir(d))
    used = [d for d in days if (not args.image or args.image in night_image(d))
            and (args.all or night_complete(d))]
    with open(os.path.join(out, "nights.txt"), "w") as f:
        for d in days:
            n = len(glob.glob(os.path.join(d, "**", "DONE"), recursive=True))
            state = "used" if d in used else ("not used: did not finish" if not night_complete(d) else "not used")
            f.write(f"{os.path.basename(d)}  {night_image(d) or '?':32s} {n:3d} runs  {state}\n")

    runs = []
    for d in used:
        for done in sorted(glob.glob(os.path.join(d, "**", "DONE"), recursive=True)):
            run = os.path.dirname(done)
            rel = os.path.relpath(run, d).split(os.sep)
            try:
                info = {"night": os.path.basename(d), "group": rel[0], "rep": rel[-1], "path": run}
                if rel[0] == "1-normal-operation":
                    info.update(profile=rel[1], config=rel[2])
                else:
                    group, experiment = experiment_name(rel[:-1]).split("/", 1)
                    info.update(group=group, profile=experiment, config="5-follow-rate-forecast")
                info.update(whole_run(run))
                info["phases"] = read_phases(run)
                info.update(outcomes(run))
                runs.append(info)
            except (OSError, KeyError, ValueError) as e:
                print(f"skipped {run}: {e}")

    cols = ["night", "group", "profile", "config", "rep", "complete", "over_limit", "within_limit_pct", "p50_ms",
            "p95_ms", "p99_ms", "max_ms", "pods_mean"] + [k for _, k in OUTCOMES]
    with open(os.path.join(out, "runs.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in runs:
            w.writerow({k: (round(v, 2) if isinstance(v, float) else v) for k, v in r.items() if k in cols})

    normal = [r for r in runs if r["group"] == "1-normal-operation"]
    profiles = sorted({r["profile"] for r in normal})

    # Normal operation, whole runs
    rows = []
    for prof in profiles:
        for cfg in CONFIGS:
            rs = [r for r in normal if r["profile"] == prof and r["config"] == cfg]
            if rs:
                rows.append([prof, SHORT[cfg], ms([r["within_limit_pct"] for r in rs]),
                             ms([r["over_limit"] for r in rs], 0), ms([r["p95_ms"] for r in rs], 0),
                             ms([r["max_ms"] for r in rs], 0), ms([r["pods_mean"] for r in rs], 2)])
    table(os.path.join(out, "normal_runs.txt"),
          ["profile", "configuration", "within limit %", "late requests", "p95 ms", "max ms", "mean ready pods"], rows)

    # Normal operation, per phase
    metrics = [("within_limit_pct", "within limit %", 1), ("p95_ms", "p95 ms", 0),
               ("p95_above_limit_s", "P95 above limit s", 0), ("pods_total", "mean ready pods", 2),
               ("switches_up", "switches up", 1), ("switches_down", "switches down", 1)]
    rows, csv_rows = [], []
    per_phase = defaultdict(lambda: defaultdict(list))   # (profile, phase) -> config -> phase rows
    phase_order = {}
    for r in normal:
        for i, ph in enumerate(r["phases"]):
            per_phase[(r["profile"], ph["phase"])][r["config"]].append(ph)
            phase_order.setdefault(r["profile"], {}).setdefault(ph["phase"], i)
    for prof in profiles:
        for phase in sorted(phase_order[prof], key=phase_order[prof].get):
            for cfg in CONFIGS:
                phs = per_phase[(prof, phase)].get(cfg, [])
                if not phs:
                    continue
                rows.append([prof, phase, SHORT[cfg]] + [ms([x[k] for x in phs], d) for k, _, d in metrics])
                rec = {"profile": prof, "phase": phase, "config": cfg, "n": len(phs)}
                for k, _, _ in metrics:
                    m, s, _ = mean_std([x[k] for x in phs])
                    rec[f"{k}_mean"], rec[f"{k}_std"] = round(m, 3), round(s, 3)
                csv_rows.append(rec)
    table(os.path.join(out, "normal_phases.txt"), ["profile", "phase", "configuration"] + [t for _, t, _ in metrics],
          rows)
    if csv_rows:
        with open(os.path.join(out, "normal_phases.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(csv_rows[0]))
            w.writeheader()
            w.writerows(csv_rows)

    # Plots: one per profile; rows are metrics, groups are phases, bars are configurations (error bar: std)
    plot_metrics = [("within_limit_pct", "within the limit (%)"), ("p95_above_limit_s", "P95 above the limit (s)"),
                    ("pods_total", "mean ready pods")]
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    for prof in profiles:
        phases = sorted(phase_order[prof], key=phase_order[prof].get)
        cfgs = [c for c in CONFIGS if any(per_phase[(prof, ph)].get(c) for ph in phases)]
        fig, axes = plt.subplots(len(plot_metrics), 1, figsize=(max(8, 1.6 * len(phases) * len(cfgs) / 3), 10),
                                 sharex=True)
        width = 0.8 / max(1, len(cfgs))
        x = np.arange(len(phases))
        nrep = 0
        for ax, (key, label) in zip(axes, plot_metrics):
            for i, cfg in enumerate(cfgs):
                stats = [mean_std([p[key] for p in per_phase[(prof, ph)].get(cfg, [])]) for ph in phases]
                nrep = max([nrep] + [s[2] for s in stats])
                ax.bar(x + (i - (len(cfgs) - 1) / 2) * width, [s[0] for s in stats], width,
                       yerr=[s[1] for s in stats], capsize=2, color=colors[i % len(colors)], label=SHORT[cfg])
            ax.set_ylabel(label)
            ax.grid(axis="y", alpha=0.3)
        axes[0].set_ylim(0, 105)
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.965), ncol=len(cfgs), fontsize=9)
        axes[-1].set_xticks(x)
        axes[-1].set_xticklabels(phases)
        fig.suptitle(f"{prof}: configurations per phase (mean ± std, up to {nrep} runs each)")
        fig.tight_layout(rect=(0, 0, 1, 0.94))
        fig.savefig(os.path.join(out, f"normal_{prof}.png"), dpi=130)
        plt.close(fig)

    # Faults and overload
    rows, csv_rows = [], []
    for exp in sorted({(r["group"], r["profile"]) for r in runs if r["group"] != "1-normal-operation"}):
        rs = [r for r in runs if (r["group"], r["profile"]) == exp]
        name = f"{exp[0]}/{exp[1]}"
        rows.append([name] + [ms([r.get(k, np.nan) for r in rs]) for _, k in OUTCOMES]
                    + [ms([r["p95_ms"] for r in rs], 0), ms([r["max_ms"] for r in rs], 0)])
        rec = {"experiment": name, "n": len(rs)}
        for _, k in OUTCOMES + [(None, "p95_ms"), (None, "max_ms")]:
            m, s, _ = mean_std([r.get(k, np.nan) for r in rs])
            rec[f"{k}_mean"], rec[f"{k}_std"] = round(m, 2), round(s, 2)
        csv_rows.append(rec)
    table(os.path.join(out, "faults.txt"),
          ["experiment", "within limit %", "above limit %", "lost %", "rejected %", "HTTP 500 %", "p95 ms", "max ms"],
          rows)
    if csv_rows:
        with open(os.path.join(out, "faults.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(csv_rows[0]))
            w.writeheader()
            w.writerows(csv_rows)

    print(open(os.path.join(out, "nights.txt")).read())
    print(open(os.path.join(out, "normal_runs.txt")).read())
    print(f"{len(runs)} runs; written to {out}")


if __name__ == "__main__":
    main()