"""Open-loop load generator: sends real camera frames to the framework app at a scheduled rate.

Two sources of frames:
  --frames DIR       every camera sends the images of DIR in turn, with the position and heading of the template
                     (all cameras are ego vehicles: vehicle ids <run>-cam0 ... <run>-camN)
  --scenario FILE    a recorded scenario from scenario_streams.py (an RSU and a car): the cameras take its streams
                     in turn, so 8 cameras are 4 RSU + car pairs; each frame carries the position, heading and speed
                     recorded with it, and the vehicle id keeps the device's id (<run>-RSU-DEMO-01-0, <run>-506-0, ...),
                     so the fusion component treats the RSU streams as RSUs and fuses them with the cars
"""
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

# Files of the frames folder that are sent as camera frames; anything else in the folder (scripts, notes) is skipped
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".bmp")


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


def encode(path):
    return base64.b64encode(open(path, "rb").read()).decode()


def folder_streams(frames_dir):
    """One stream of the images of a folder, without metadata (the template's position and heading are kept)"""
    files = [f for f in sorted(glob.glob(os.path.join(frames_dir, "*"))) if f.lower().endswith(IMAGE_EXTENSIONS)]
    if not files:
        raise SystemExit(f"no camera frames ({', '.join(IMAGE_EXTENSIONS)}) in {frames_dir}")
    return [{"vehicle_id": "cam", "frames": [(encode(f), None) for f in files]}]


def scenario_streams(path):
    """The streams of a recorded scenario, each frame with the metadata recorded with it"""
    scenario = json.load(open(os.path.expanduser(path)))
    return [{"vehicle_id": s["vehicle_id"], "frames": [(encode(f["image"]), f["metadata"]) for f in s["frames"]]}
            for s in scenario["streams"]]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://192.168.49.2:30862/")
    p.add_argument("--host", required=True, help="app stable host name")
    p.add_argument("--profile", required=True, help="e.g. 60:2 or 60:2,120:2-14,120:14")
    p.add_argument("--cameras", type=int, default=8)
    p.add_argument("--frames", default="tools/tracker-proto/frames")
    p.add_argument("--scenario", help="recorded scenario (scenario_streams.py output) instead of --frames")
    p.add_argument("--template", default=os.path.expanduser("~/event-detect.json"))
    p.add_argument("--run-id", default=datetime.now().strftime("run%H%M%S"))
    p.add_argument("--workers", type=int, default=64, help="concurrent requests in flight (open loop needs rate x latency)")
    p.add_argument("--out", required=True, help="output directory of the run")
    args = p.parse_args()

    template = json.load(open(args.template))
    streams = scenario_streams(args.scenario) if args.scenario else folder_streams(args.frames)
    times = schedule(parse_profile(args.profile))
    headers = {"Host": args.host, "Content-Type": "application/json",
               "X-Forward-To": "detect-track-service"}
    rows, lock = [], threading.Lock()
    session = requests.Session()

    def vehicle_id(camera):
        """A stream of its own per camera: the cameras take the streams in turn, a group of them per round"""
        stream = streams[camera % len(streams)]
        if args.scenario:
            return f"{args.run_id}-{stream['vehicle_id']}-{camera // len(streams)}"
        return f"{args.run_id}-cam{camera}"

    def send(seq, t_sched):
        camera = seq % args.cameras
        stream = streams[camera % len(streams)]
        image, metadata = stream["frames"][(seq // args.cameras) % len(stream["frames"])]
        event = copy.deepcopy(template)
        event["vehicle_id"] = vehicle_id(camera)
        event["timestamp"] = datetime.now(timezone.utc).isoformat()
        event["data"]["image1"] = image
        if metadata:
            event.setdefault("metadata", {}).update(metadata)
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

    source = f"scenario {args.scenario}" if args.scenario else f"frames {args.frames}"
    print(f"{args.run_id}: {len(times)} requests over {times[-1]:.0f} s, {args.cameras} cameras, {source}: "
          + ", ".join(sorted({vehicle_id(c) for c in range(args.cameras)})))
    start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
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
