from event import Event
import asyncio
import time
from typing import Any, Dict
from position_core import PositionCalculator, MONITORING_ENABLED, tsdb, extract_message_id, logger

# Position handler for Egon's FaaS framework (component "position-service-2d").
# Wraps the pure calculation + Redis-enrichment logic of position_core without
# the RabbitMQ message/consumer wrapping.

_position_calculator = PositionCalculator()


def handler(event: Event) -> Dict[str, Any]:
    """Contains the logic of PositionCalculator.process_2d_message(),
    without the RabbitMQ message wrapping."""
    fusion_data = event.json or {}

    message_id = None
    if MONITORING_ENABLED and tsdb:
        message_id = extract_message_id(fusion_data)

    logger.info(f"Received 2D fusion data for processing: {fusion_data.get('vehicle_id', 'unknown')}")
    start_time = time.time()

    # Calculate positions from 2D data
    position_data = _position_calculator.calculate_2d_to_position(fusion_data)

    # Enrich with track history (heading, movement, is_moving). This is an
    # async method in the original ADAS code, even though it only performs
    # synchronous Redis calls internally - asyncio.run() lets us call it
    # from this plain synchronous handler without rewriting its body.
    position_data = asyncio.run(
        _position_calculator.enrich_and_store_track_data(position_data, fusion_data)
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
            vehicle_id=fusion_data.get("vehicle_id"),
            queue_name="handler-2d",
            metadata={
                "positions_count": len(position_data.get("positions", [])),
                "data_type": "2d",
            },
        )

    logger.info(f"Processed 2D position calculation for vehicle {position_data.get('vehicle_id', 'unknown')}")
    return position_data
