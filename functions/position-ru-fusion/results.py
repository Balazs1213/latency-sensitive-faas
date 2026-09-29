from redis import Redis
import os
import json
from datetime import datetime, timezone
from config import APP_NAME
from typing import Any
import time


RESULT_STORE_ADDRESS = os.environ["RESULT_STORE_ADDRESS"]
redis_client = Redis(host=RESULT_STORE_ADDRESS, port=6379)

PERF_KEY = f"perf:{APP_NAME}"


def write_result(event: Any, correlation_id: str = "") -> None:
    # Write the result event to Redis
    key = f"result:{APP_NAME}"
    timestamp = datetime.now(timezone.utc).isoformat()
    value = {
        "timestamp": timestamp,
        "event": event,
    }
    redis_client.rpush(key, json.dumps(value))
    # Keep only the last 10 results for memory efficiency
    redis_client.ltrim(key, -10, -1)

    # For evaluation purposes, we also log the write time with correlation ID
    if correlation_id:
        perf_data = {
            "correlation_id": correlation_id,
            "timestamp": timestamp,
        }

        # We push the performance data to a separate, dedicated list
        redis_client.rpush(PERF_KEY, json.dumps(perf_data))
        # Keep only the last 5000 performance entries for memory efficiency
        redis_client.ltrim(PERF_KEY, -5000, -1)
