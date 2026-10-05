import os

# Point both modules' Redis clients at the port-forwarded ADAS Redis.
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")

from event import Event
from position_service import handler as position_service_handler
from ru_fusion_service import handler as ru_fusion_handler

# This simulates the "local" routing chain that Egon's func.py would perform:
# position-service -> ru-fusion-service, within a single composed function
# invocation, passing the output of the first handler as the input to the
# second one.

# Vehicle id contains "rsu" on purpose: ru_fusion_handler only runs the
# actual fusion logic for RSU-sourced observations (see ru_fusion.py).
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

print("=" * 70)
print("STAGE 1: position-service (raw detection -> GPS position)")
print("=" * 70)
event_in = Event(json=sample_2d_input, data=None)
position_result = position_service_handler(event_in)
print(position_result)

print()
print("=" * 70)
print("STAGE 2: ru-fusion-service (position -> fused road-user object)")
print("=" * 70)
event_fusion_in = Event(json=position_result, data=None)
fusion_result = ru_fusion_handler(event_fusion_in)
print(fusion_result)

print()
print("=" * 70)
print("CHAIN COMPLETE: position-service -> ru-fusion-service")
print("=" * 70)
