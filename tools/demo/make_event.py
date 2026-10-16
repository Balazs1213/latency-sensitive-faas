import base64
import json
import sys
from datetime import datetime

# Build one camera-stream style event from a frame file: python3 make_event.py <frame.jpg>
with open(sys.argv[1], "rb") as f:
    image_b64 = base64.b64encode(f.read()).decode()

event = {
    "vehicle_id": "RSU-TEST-01",
    "timestamp": datetime.utcnow().isoformat(),
    "data": {"image1": image_b64},
    "metadata": {
        "position": {"lat": 47.4720437, "lon": 19.0596061, "alt": 150.5},
        "heading": 70.9,
        "speed": 0.0,
        "vehicle": {"height_m": 1.5, "width_m": 2.0},
        "camera_calibration": {"sensor_height_mm": 20, "focal_length_mm": 35, "aov": 63},
    },
}
json.dump(event, sys.stdout)