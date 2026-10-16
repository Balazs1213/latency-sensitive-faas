import base64
import glob
import os
import time

os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6390")

from PIL import Image
from ultralytics import YOLO
from ultralytics.trackers.basetrack import BaseTrack

import detect_track_service as svc

FRAMES = sorted(glob.glob("../../tools/tracker-proto/frames/*.jpg"))
META = {
    "position": {"lat": 47.0, "lon": 19.0, "alt": 150.0},
    "heading": 70.0,
    "vehicle": {"height_m": 1.5, "width_m": 2.0},
    "camera_calibration": {"sensor_height_mm": 20, "focal_length_mm": 35, "aov": 63},
}

# 1) The new handler: state goes through Redis on every frame
svc._redis.delete("tracker-state:test-cam")
handler_ids, times_ms = [], []
for path in FRAMES:
    with open(path, "rb") as f:
        image_b64 = base64.b64encode(f.read()).decode()
    event = {"vehicle_id": "test-cam", "timestamp": 0, "data": {"image1": image_b64}, "metadata": META}
    t0 = time.perf_counter()
    out = svc.handler(event)
    times_ms.append((time.perf_counter() - t0) * 1000)
    handler_ids.append(sorted(t["track_id"] for t in out["tracks"]))

# 2) Reference: the original way, tracker kept in memory
BaseTrack.reset_id()
ref = YOLO(os.getenv("YOLO_MODEL", "yolov8n.pt"))
ref_ids = []
for path in FRAMES:
    r = ref.track(Image.open(path), persist=True, verbose=False, tracker=svc.TRACKER)[0]
    ref_ids.append([] if r.boxes.id is None else sorted(r.boxes.id.int().tolist()))

same = sum(1 for a, b in zip(handler_ids, ref_ids) if a == b)
print(f"frames: {len(FRAMES)}, frames with tracks: {sum(1 for x in ref_ids if x)}")
print(f"identical track ids (handler vs original): {same}/{len(FRAMES)}")
print(f"handler time avg: {sum(times_ms) / len(times_ms):.1f} ms")
for i, (a, b) in enumerate(zip(handler_ids, ref_ids)):
    if a != b:
        print(f"diff at frame {i}: handler {a}, original {b}")