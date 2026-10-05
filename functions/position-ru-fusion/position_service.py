from event import Event
import asyncio
import time
from typing import Any, Dict, List, Optional
from position_core import PositionCalculator, MONITORING_ENABLED, tsdb, extract_message_id, logger

# Position handler for Egon's FaaS framework (component "position-service").
# Wraps the pure calculation + Redis-enrichment logic of position_core without
# the RabbitMQ message/consumer wrapping. The original service consumed two
# queues with different payload shapes (detection-tracking-results and
# 3d-tracking-results); here a single handler picks the branch from the shape
# of the detections.

_position_calculator = PositionCalculator()

# Detection keys that only a 3D detector produces. Checked before the 2D key,
# because a 3D producer may also include the 2D-compatible fields.
_3D_KEYS = ("location", "dimensions", "bbox_3d")
_2D_KEY = "relative_position"


def detect_input_kind(detections: List[Dict[str, Any]]) -> Optional[str]:
    """Returns "3d", "2d", or None if there are no detections."""
    if not detections:
        return None
    if any(key in d for d in detections for key in _3D_KEYS):
        return "3d"
    if any(_2D_KEY in d for d in detections):
        return "2d"
    raise ValueError(
        f"Unrecognised detection format: expected one of {_3D_KEYS} (3D) or '{_2D_KEY}' (2D)"
    )


def handler(event: Event) -> Dict[str, Any]:
    """Contains the logic of PositionCalculator.process_2d_message() and
    process_3d_message(), without the RabbitMQ message wrapping."""
    data = event.json or {}
    kind = detect_input_kind(data.get("detections", []))

    message_id = None
    if MONITORING_ENABLED and tsdb:
        message_id = extract_message_id(data)

    logger.info(f"Received {kind or 'empty'} detection data for processing: {data.get('vehicle_id', 'unknown')}")
    start_time = time.time()

    # Without detections the 2D branch yields an empty "positions" list.
    if kind == "3d":
        position_data = _position_calculator.calculate_3d_to_position(data)
    else:
        position_data = _position_calculator.calculate_2d_to_position(data)

    # Enrich with track history (heading, movement, is_moving). This is an
    # async method in the original ADAS code, even though it only performs
    # synchronous Redis calls internally - asyncio.run() lets us call it
    # from this plain synchronous handler without rewriting its body.
    position_data = asyncio.run(
        _position_calculator.enrich_and_store_track_data(position_data, data)
    )

    if message_id:
        position_data["message_id"] = message_id

    # Record processing time (needed for the cold-start / latency measurements)
    if MONITORING_ENABLED and tsdb and message_id:
        tsdb.record_message_processing(
            message_id=message_id,
            service_name="position-service",
            processing_time_ms=int((time.time() - start_time) * 1000),
            status="success",
            vehicle_id=data.get("vehicle_id"),
            queue_name=f"handler-{kind or 'empty'}",
            metadata={
                "positions_count": len(position_data.get("positions", [])),
                "data_type": kind or "empty",
            },
        )

    logger.info(f"Processed {kind or 'empty'} position calculation for vehicle {position_data.get('vehicle_id', 'unknown')}")
    return position_data
