"""Open-loop load generator: sends real camera frames to the framework app at a scheduled rate."""
import argparse
import base64
import copy
import csv
import glob
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests


def parse_profile(text):
    """Parse 'dur:rate' or 'dur:start-end' segments, e.g. '60:2,120:2-14,120:14'."""
    segments = []
    for part in text.split(","):
        dur, rates = part.split(":")
        start, _, end = rates.partition("-")
        segments.append((float(dur), float(start), float(end or start)))
    return segments


def schedule(segments):
    """Send times (s from start) following piecewise-linear rates."""
    times, offset = [], 0.0
    for dur, r0, r1 in segments:
        t = 0.0
        while t < dur:
            rate = r0 + (r1 - r0) * t / dur
            if rate <= 0:
                t += 0.1
                continue
            times.append(offset + t)
            t += 1.0 / rate
        offset += dur
    return times


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://192.168.49.2:30862/")
    p.add_argument("--host", required=True, help="app stable host name")
    p.add_argument("--profile", required=True, help="e.g. 60:2 or 60:2,120:2-14,120:14")
    p.add_argument("--cameras", type=int, default=8)
    p.add_argument("--frames", default="tools/tracker-proto/frames")
    p.add_argument("--template", default=os.path.expanduser("~/event-detect.json"))
    p.add_argument("--run-id", default=datetime.now().strftime("run%H%M%S"))
    p.add_argument("--out", required=True, help="output directory of the run")
    args = p.parse_args()

    template = json.load(open(args.template))
    frames = [base64.b64encode(open(f, "rb").read()).decode()
              for f in sorted(glob.glob(os.path.join(args.frames, "*")))]
    times = schedule(parse_profile(args.profile))
    headers = {"Host": args.host, "Content-Type": "application/json",
               "X-Forward-To": "detect-track-service"}
    rows, lock = [], threading.Lock()
    session = requests.Session()

    def send(seq, t_sched):
        camera = seq % args.cameras
        event = copy.deepcopy(template)
        event["vehicle_id"] = f"{args.run_id}-cam{camera}"
        event["timestamp"] = datetime.now(timezone.utc).isoformat()
        event["data"]["image1"] = frames[(seq // args.cameras) % len(frames)]
        corr_id = f"{args.run_id}-{seq}"
        t_send = time.time()
        try:
            r = session.post(args.url, json=event, timeout=30,
                             headers={**headers, "X-Correlation-ID": corr_id})
            status = r.status_code
        except Exception:
            status = 0  # connection error or timeout
        t_done = time.time()
        with lock:
            rows.append({"seq": seq, "camera": camera, "t_sched": round(t_sched, 3),
                         "t_send": round(t_send, 3), "t_done": round(t_done, 3), "status": status,
                         "client_ms": round((t_done - t_send) * 1000, 1), "corr_id": corr_id})

    print(f"{args.run_id}: {len(times)} requests over {times[-1]:.0f} s, {args.cameras} cameras")
    start = time.time()
    with ThreadPoolExecutor(max_workers=64) as pool:
        for seq, offset in enumerate(times):
            delay = start + offset - time.time()
            if delay > 0:
                time.sleep(delay)  # open loop: never wait for responses
            pool.submit(send, seq, start + offset)

    os.makedirs(args.out, exist_ok=True)
    rows.sort(key=lambda r: r["seq"])
    with open(os.path.join(args.out, "loadgen.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    ok = sum(1 for r in rows if r["status"] == 200)
    print(f"done: {len(rows)} sent, {ok} HTTP 200, start {datetime.fromtimestamp(start, timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()