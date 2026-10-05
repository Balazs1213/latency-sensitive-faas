import os

# Point the module's Redis client at the port-forwarded ADAS Redis
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")

from event import Event
from ru_fusion_service import handler as ru_fusion_handler

# Sample input, matching the real structure that position-service
# sends to ru-fusion-service (taken from an earlier live pipeline run)
sample_input = {
    "vehicle_id": "RSU-DEMO-01",
    "timestamp": "2026-09-29T11:27:13.619395",
    "positions": [
        {
            "track_id": 1,
            "class": "person",
            "position_3d": {
                "lat": 47.4720386,
                "lon": 19.059602,
                "movement_heading": 90.0,
                "is_moving": True,
            },
            "confidence": 0.85,
            "bbox_2d": [420, 370, 450, 460],
        }
    ],
    "metadata": {
        "position": {"lat": 47.4720386, "lon": 19.059602, "alt": 149.7},
        "heading": 68.4,
    },
}

fake_event = Event(json=sample_input, data=None)
result = ru_fusion_handler(fake_event)

print("\n=== HANDLER RESULT ===")
print(result)
