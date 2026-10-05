"""
Local end-to-end run of the position-ru-fusion composition through the real
func.py orchestrator (parliament server), not by chaining handlers by hand.

Chain under test ("local" route in one composition):
    position-service -> ru-fusion-service
position-service picks its 2D or 3D branch from the payload shape; both
branches are exercised.

What it does:
  1. Writes the routing table below into the routing-table Redis under
     ROUTING_KEY (must equal the server's FUNCTION_NAME).
  2. POSTs a 2D and a 3D sample to the server with a fresh timestamp.
  3. Checks for each branch: HTTP 200, a new entry at the end of
     result:<APP_NAME> carrying our timestamp, our correlation id in
     perf:<APP_NAME>, and that the fused object is still present in the ADAS
     Redis right after the request (i.e. it did not expire immediately).

Timestamp format: timezone-aware UTC ISO 8601,
datetime.now(timezone.utc).isoformat(), e.g. "2026-10-05T10:30:00.123456+00:00".
ru_fusion stores the message timestamp as the fused object's last_update and
expire_stale_objects() compares datetime.fromisoformat(last_update).timestamp()
with time.time(). An aware timestamp converts to the correct epoch regardless
of the server's local timezone. A naive one (datetime.now().isoformat(), as the
handlers use as default) is interpreted in the server's local time, so it only
works if this script and the server share a timezone. Note FUSED_OBJECT_TTL_S
defaults to 2 s, so the fused object is only expected to survive briefly; the
check runs immediately after each request.

Prerequisites (this is a script, not a pytest test):
  kubectl port-forward -n redis svc/redis-master 6379:6379   # routing + results
  kubectl port-forward -n default svc/redis 6380:6379        # ADAS handler state

  # Server, from this directory:
  NODE_IP=localhost RESULT_STORE_ADDRESS=localhost \
  APP_NAME=position-ru-fusion FUNCTION_NAME=position-ru-fusion-local \
  REDIS_HOST=localhost REDIS_PORT=6380 \
  venv/bin/python -m parliament .

Run:
  venv/bin/python test_local_e2e.py

Optional env overrides for this script: FUNC_URL (default http://localhost:8080),
ROUTING_REDIS_HOST/ROUTING_REDIS_PORT (default localhost:6379),
ADAS_REDIS_HOST/ADAS_REDIS_PORT (default localhost:6380),
APP_NAME (default position-ru-fusion), ROUTING_KEY (default position-ru-fusion-local).
Exit code is 0 if all checks pass, 1 otherwise.

Keys this script creates/updates in the routing-table Redis: ROUTING_KEY,
result:<APP_NAME>, perf:<APP_NAME>. In the ADAS Redis the handlers write
vehicle:*, fused:* keys as usual.
"""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from redis import Redis

FUNC_URL = os.getenv("FUNC_URL", "http://localhost:8080")
APP_NAME = os.getenv("APP_NAME", "position-ru-fusion")
ROUTING_KEY = os.getenv("ROUTING_KEY", "position-ru-fusion-local")

routing_redis = Redis(
    host=os.getenv("ROUTING_REDIS_HOST", "localhost"),
    port=int(os.getenv("ROUTING_REDIS_PORT", "6379")),
)
adas_redis = Redis(
    host=os.getenv("ADAS_REDIS_HOST", "localhost"),
    port=int(os.getenv("ADAS_REDIS_PORT", "6380")),
)

RESULT_KEY = f"result:{APP_NAME}"
PERF_KEY = f"perf:{APP_NAME}"
FUSED_OBJ_PREFIX = "fused:obj:"  # see ru_fusion.FUSED_OBJ_PREFIX

ROUTING_TABLE = {
    "position-service": [{"component": "ru-fusion-service", "url": "local"}],
    "ru-fusion-service": [],
}

# Vehicle id contains "rsu" on purpose: ru-fusion-service only fuses
# RSU-sourced observations.
METADATA = {
    "position": {"lat": 47.4720386, "lon": 19.059602, "alt": 149.7},
    "heading": 68.4,
}


def sample_2d(timestamp: str) -> Dict[str, Any]:
    # Same payload as test_manual_chain.py, with a fresh timestamp.
    return {
        "vehicle_id": "RSU-DEMO-01",
        "timestamp": timestamp,
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
        "metadata": METADATA,
    }


def sample_3d(timestamp: str) -> Dict[str, Any]:
    # Same payload as the 3D sample in test_manual_position.py, with a fresh timestamp.
    return {
        "vehicle_id": "RSU-DEMO-01",
        "timestamp": timestamp,
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
        "tracks": [{"track_id": 1, "bbox_2d": [420, 370, 450, 460]}],
        "metadata": METADATA,
    }


def run_branch(component: str, branch: str, payload: Dict[str, Any]) -> List[str]:
    """POSTs one sample through func.py and returns the list of failed checks."""
    failures: List[str] = []
    label = f"{component} [{branch}]"
    correlation_id = f"local-e2e-{component}-{branch}-{uuid.uuid4().hex[:8]}"
    last_before = routing_redis.lindex(RESULT_KEY, -1)

    resp = requests.post(
        FUNC_URL,
        json=payload,
        headers={"X-Forward-To": component, "X-Correlation-ID": correlation_id},
        timeout=30,
    )
    # Check the fused objects first: their TTL is short (2 s by default).
    last_after = routing_redis.lindex(RESULT_KEY, -1)
    result = json.loads(last_after)["event"] if last_after else {}
    fused_ids = [p.get("fused_id") for p in result.get("positions", [])]
    fused_present = {
        fid: adas_redis.exists(f"{FUSED_OBJ_PREFIX}{fid}") == 1 for fid in fused_ids
    }

    print(f"--- {label} (correlation id {correlation_id})")
    print(f"HTTP {resp.status_code}: {resp.text.strip()}")
    print(f"fused objects in ADAS Redis: {fused_present}")

    if resp.status_code != 200:
        failures.append(f"{label}: HTTP {resp.status_code}")
    # result:<app> is trimmed to 10 entries, so check for a new tail entry
    # with our timestamp instead of comparing lengths.
    if last_after is None or last_after == last_before:
        failures.append(f"{label}: no new entry in {RESULT_KEY}")
    elif result.get("timestamp") != payload["timestamp"]:
        failures.append(f"{label}: last {RESULT_KEY} entry is not from this request")
    if not fused_ids:
        failures.append(f"{label}: result has no fused objects")
    elif not all(fused_present.values()):
        failures.append(f"{label}: fused object expired immediately: {fused_present}")
    perf_entries = [json.loads(e) for e in routing_redis.lrange(PERF_KEY, -10, -1)]
    if correlation_id not in [e.get("correlation_id") for e in perf_entries]:
        failures.append(f"{label}: correlation id missing from {PERF_KEY}")
    return failures


def main() -> int:
    routing_redis.set(ROUTING_KEY, json.dumps(ROUTING_TABLE))
    print(f"Routing table set under '{ROUTING_KEY}': {routing_redis.get(ROUTING_KEY).decode()}")

    failures: List[str] = []
    for branch, make_sample in (("2d", sample_2d), ("3d", sample_3d)):
        timestamp = datetime.now(timezone.utc).isoformat()
        failures += run_branch("position-service", branch, make_sample(timestamp))

    print()
    if failures:
        print("FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
