import os

# Point the position-service module's own Redis client at the port-forwarded ADAS Redis.
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")

from event import Event
from position_service import handler as position_service_handler, detect_input_kind

# --- 2D sample input, matching what 2d-detect-and-track publishes ---
sample_2d_input = {
    "vehicle_id": "RSU-DEMO-01",
    "timestamp": "2026-10-02T11:00:00.000000",
    "detections": [
        {
            "class": "person",
            "confidence": 0.85,
            "bbox_2d": [420, 370, 450, 460],
            "relative_position": {
                "distance_m": 5.0,
                "direction_angle_rad": 0.5,
                "direction_angle_deg": 28.6,
            },
        }
    ],
    "metadata": {
        "position": {"lat": 47.4720386, "lon": 19.059602, "alt": 149.7},
        "heading": 68.4,
    },
}

event_2d = Event(json=sample_2d_input, data=None)
assert detect_input_kind(sample_2d_input["detections"]) == "2d"
result_2d = position_service_handler(event_2d)
print("\n=== 2D HANDLER RESULT ===")
print(result_2d)

# --- 3D sample input, matching what a 3D detection/tracking source would send ---
sample_3d_input = {
    "vehicle_id": "RSU-DEMO-01",
    "timestamp": "2026-10-02T11:00:01.000000",
    "detections": [
        {
            "track_id": 1,
            "class": "person",
            "location": [2.0, 0.0, 8.0],
            "dimensions": [0.5, 0.5, 1.8],
            "rotation_y": 0.3,
            "bbox_2d": [420, 370, 450, 460],
        }
    ],
    "tracks": [
        {"track_id": 1, "bbox_2d": [420, 370, 450, 460]},
    ],
    "metadata": {
        "position": {"lat": 47.4720386, "lon": 19.059602, "alt": 149.7},
        "heading": 68.4,
    },
}

event_3d = Event(json=sample_3d_input, data=None)
assert detect_input_kind(sample_3d_input["detections"]) == "3d"
result_3d = position_service_handler(event_3d)
print("\n=== 3D HANDLER RESULT ===")
print(result_3d)

# --- No detections: the handler must return an empty positions list ---
sample_empty_input = {**sample_2d_input, "detections": [], "tracks": []}
result_empty = position_service_handler(Event(json=sample_empty_input, data=None))
print("\n=== EMPTY INPUT RESULT ===")
print(result_empty)
assert result_empty["positions"] == []

assert result_2d["positions"] and result_3d["positions"]
print("\nALL POSITION CHECKS PASSED")
