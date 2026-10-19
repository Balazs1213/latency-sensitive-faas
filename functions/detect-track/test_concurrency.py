import base64
import glob
import os
import threading

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


def load_b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()


IMAGES = [load_b64(p) for p in FRAMES]


def run_camera(vehicle_id, out):
    """One camera: its frames in order, as the bridge would send them."""
    svc._redis.delete(f"tracker-state:{vehicle_id}")
    for img in IMAGES:
        event = {"vehicle_id": vehicle_id, "timestamp": 0, "data": {"image1": img}, "metadata": META}
        res = svc.handler(event)
        out.append(sorted(t["track_id"] for t in res["tracks"]))


# Two cameras at the same time in one process, like concurrent requests in one replica
results = {"cam-A": [], "cam-B": []}
threads = [threading.Thread(target=run_camera, args=(cam, results[cam])) for cam in results]
for t in threads:
    t.start()
for t in threads:
    t.join()

# Reference: original in-memory tracking of a single camera
BaseTrack.reset_id()
ref = YOLO(os.getenv("YOLO_MODEL", "yolov8n.pt"))
ref_ids = []
for path in FRAMES:
    r = ref.track(Image.open(path), persist=True, verbose=False, tracker=svc.TRACKER)[0]
    ref_ids.append([] if r.boxes.id is None else sorted(r.boxes.id.int().tolist()))

for cam, ids in results.items():
    same = sum(1 for a, b in zip(ids, ref_ids) if a == b)
    print(f"{cam}: processed {len(ids)} frames, identical track ids {same}/{len(ref_ids)}")