from event import Event
import time
from datetime import datetime
from typing import Any, Dict
from ru_fusion import (
    MONITORING_ENABLED,
    tsdb,
    extract_message_id,
    logger,
    upsert_ego_vehicle,
    fuse_observations,
    expire_stale_objects,
    build_fusion_result,
)

# Fusion handler for Egon's FaaS framework (component "ru-fusion-service").
# Wraps the fusion logic of ru_fusion without the RabbitMQ message/consumer
# wrapping.


def handler(event: Event) -> Dict[str, Any]:
    """Contains the logic of ru_fusion.process_message_and_fuse(),
    without the RabbitMQ message wrapping."""
    data = event.json or {}

    message_id = None
    if MONITORING_ENABLED and tsdb:
        message_id = extract_message_id(data)

    vehicle_id = data.get("vehicle_id", "unknown")
    timestamp = data.get("timestamp", datetime.now().isoformat())
    positions = data.get("positions", [])
    metadata = data.get("metadata", {})

    logger.info(f"Fusing {len(positions)} observations from ego {vehicle_id}")
    start_time = time.time()

    # 1. Register / update the ego vehicle
    upsert_ego_vehicle(vehicle_id, metadata, timestamp)

    # 2. Run fusion (only for RSU-sourced observations)
    if "rsu" in vehicle_id.lower():
        fused_objects = fuse_observations(positions, vehicle_id, timestamp)
    else:
        fused_objects = []

    # 3. Expire stale objects
    removed = expire_stale_objects()
    if removed:
        logger.info(f"Expired {removed} stale fused objects")

    # 4. Build the result
    result = build_fusion_result(vehicle_id, timestamp, fused_objects, metadata)
    if message_id:
        result["message_id"] = message_id

    # Record processing time (needed for the cold-start analysis)
    if MONITORING_ENABLED and tsdb and message_id:
        tsdb.record_message_processing(
            message_id=message_id,
            service_name="ru-fusion",
            processing_time_ms=int((time.time() - start_time) * 1000),
            status="success",
            vehicle_id=vehicle_id,
            queue_name="handler",
            metadata={
                "fused_objects_count": len(fused_objects),
                "expired_count": removed,
            },
        )

    return result
