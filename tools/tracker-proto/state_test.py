import glob
import os
import pickle
import time

import redis
from ultralytics import YOLO
from ultralytics.trackers.basetrack import BaseTrack

FRAMES = sorted(glob.glob("frames/*.jpg"))
KEY = "tracker-state:d2"
TRACKER = os.getenv("TRACKER", "botsort.yaml")
r = redis.Redis(host="localhost", port=6390)


def run(externalize):
    """Track all frames; if externalize, keep the tracker state only in Redis between frames."""
    model = YOLO("yolov8n.pt")
    BaseTrack.reset_id()
    r.delete(KEY)
    ids_per_frame, sizes, save_ms, load_ms = [], [], [], []

    for i, path in enumerate(FRAMES):
        if externalize and i > 0:
            # Restore the state as a fresh instance would do
            t0 = time.perf_counter()
            state = pickle.loads(r.get(KEY))
            model.predictor.trackers = [state["tracker"]]
            BaseTrack._count = state["next_id"]
            load_ms.append((time.perf_counter() - t0) * 1000)

        res = model.track(path, persist=True, verbose=False, tracker=TRACKER)[0]
        ids = [] if res.boxes.id is None else sorted(res.boxes.id.int().tolist())
        ids_per_frame.append(ids)

        if externalize:
            # Save the state to Redis, then forget it locally
            t0 = time.perf_counter()
            blob = pickle.dumps({"tracker": model.predictor.trackers[0], "next_id": BaseTrack._count})
            r.set(KEY, blob)
            save_ms.append((time.perf_counter() - t0) * 1000)
            sizes.append(len(blob))
            model.predictor.trackers = [None]
            BaseTrack._count = 0

    return ids_per_frame, sizes, save_ms, load_ms


a, _, _, _ = run(externalize=False)
b, sizes, save_ms, load_ms = run(externalize=True)

same = sum(1 for x, y in zip(a, b) if x == y)
print(f"frames: {len(a)}, frames with tracks: {sum(1 for x in a if x)}")
print(f"identical track ids: {same}/{len(a)}")
print(f"state size (bytes): min {min(sizes)}, max {max(sizes)}")
print(f"save avg: {sum(save_ms) / len(save_ms):.2f} ms, load avg: {sum(load_ms) / len(load_ms):.2f} ms")
for i, (x, y) in enumerate(zip(a, b)):
    if x != y:
        print(f"diff at frame {i}: original {x}, externalized {y}")