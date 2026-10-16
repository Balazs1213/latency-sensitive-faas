import base64
import io
import os
import pickle
from datetime import datetime

import numpy as np
import redis
from PIL import Image
from ultralytics import YOLO
from ultralytics.trackers.basetrack import BaseTrack

REDIS_HOST = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
TRACKER = os.getenv("TRACKER", "botsort.yaml")
STATE_TTL_S = int(os.getenv("TRACKER_STATE_TTL_S", "300"))
HISTORY_LEN = 30

OBJ_HEIGHT_MM = {"person": 1800, "car": 1500, "truck": 2500, "bus": 2500,
                 "motorcycle": 1500, "bicycle": 1700}

_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
_model = YOLO(os.getenv("YOLO_MODEL", "yolov8n.pt"))

# Warm-up: creates the predictor and a tracker, then keep a clean copy of the tracker
_model.track(np.zeros((640, 640, 3), dtype=np.uint8), persist=True, verbose=False, tracker=TRACKER)
_model.predictor.trackers[0].reset()
_FRESH_TRACKER = pickle.dumps(_model.predictor.trackers[0])


def object_properties(x1, y1, x2, y2, confidence, class_name, image_w, image_h, calib, heading):
    """Distance and direction of an object (same formula as the original service)."""
    obj_h_mm = OBJ_HEIGHT_MM.get(class_name, 1600)
    obj_h_px = abs(y2 - y1)
    distance_mm = (calib["focal_length_mm"] * obj_h_mm * image_h) / (obj_h_px * calib["sensor_height_mm"])
    center_x = (x1 + x2) / 2
    angle = ((center_x - image_w / 2) / (image_w / 2) * calib["aov"] / 2 + heading) % 360
    return {
        "class": class_name,
        "confidence": confidence,
        "bbox_2d": [x1, y1, x2, y2],
        "relative_position": {
            "distance_m": distance_mm / 1000,
            "direction_angle_rad": angle * np.pi / 180,
            "direction_angle_deg": angle,
        },
    }


def load_state(vehicle_id):
    """Tracker, next track id and track history of one camera, from Redis."""
    blob = _redis.get(f"tracker-state:{vehicle_id}")
    if blob is None:
        return pickle.loads(_FRESH_TRACKER), 0, {}
    state = pickle.loads(blob)
    return state["tracker"], state["next_id"], state["history"]


def save_state(vehicle_id, tracker, next_id, history):
    blob = pickle.dumps({"tracker": tracker, "next_id": next_id, "history": history})
    _redis.set(f"tracker-state:{vehicle_id}", blob, ex=STATE_TTL_S)


def handler(event):
    vehicle_id = event["vehicle_id"]
    meta = event["metadata"]
    calib = meta["camera_calibration"]
    image = Image.open(io.BytesIO(base64.b64decode(event["data"]["image1"])))
    image_w, image_h = image.size

    # Restore this camera's tracker before running the model
    tracker, next_id, history = load_state(vehicle_id)
    _model.predictor.trackers = [tracker]
    BaseTrack._count = next_id

    result = _model.track(image, persist=True, verbose=False, tracker=TRACKER)[0]

    detections, tracks = [], []
    if result.boxes is not None:
        for box in result.boxes:
            class_name = result.names[int(box.cls[0])]
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            det = object_properties(x1, y1, x2, y2, float(box.conf[0]), class_name,
                                    image_w, image_h, calib, meta["heading"])
            detections.append(det)
            if box.is_track:
                track_id = box.id.int().cpu().item()
                det["track_id"] = track_id
                x, y, w, h = box.xywh[0].tolist()
                track = history.setdefault(track_id, [])
                track.append((float(x), float(y), float(w), float(h)))
                del track[:-HISTORY_LEN]
                tracks.append({
                    "track_id": track_id,
                    "current_position": {"x": float(x), "y": float(y), "width": float(w), "height": float(h)},
                    "track_history": track,
                    "track_length": len(track),
                })

    save_state(vehicle_id, _model.predictor.trackers[0], BaseTrack._count, history)

    output = {
        "vehicle_id": vehicle_id,
        "timestamp": event["timestamp"],
        "detections": detections,
        "tracks": tracks,
        "metadata": {
            "image_size": [image_w, image_h],
            "num_detections": len(detections),
            "num_tracks": len(tracks),
            "vehicle_position": meta["position"],
            "vehicle_heading_angle": meta["heading"],
            "camera_calibration": calib,
        },
        "processed_at": datetime.now().isoformat(),
    }
    output["metadata"].update(meta)
    if "message_id" in event:
        output["message_id"] = event["message_id"]
    return output