from event import Event
import asyncio
import time
from typing import Any, Dict
from position_core import PositionCalculator, MONITORING_ENABLED, tsdb, extract_message_id, logger

# Position handler for Egon's FaaS framework (component "position-service-3d").
# Wraps the pure calculation + Redis-enrichment logic of position_core without
# the RabbitMQ message/consumer wrapping.

_position_calculator = PositionCalculator()


def handler(event: Event) -> Dict[str, Any]:
    """Contains the logic of PositionCalculator.process_3d_message(),
    without the RabbitMQ message wrapping."""
    detection_data = event.json or {}

    message_id = None
    if MONITORING_ENABLED and tsdb:
        message_id = extract_message_id(detection_data)

    logger.info(f"Received 3D detection data for processing: {detection_data.get('vehicle_id', 'unknown')}")
    start_time = time.time()

    # Calculate positions from 3D data
    position_data = _position_calculator.calculate_3d_to_position(detection_data)

    # Enrich with track history (heading, movement, is_moving) - see the note
    # in position_service_2d.handler about asyncio.run().
    position_data = asyncio.run(
        _position_calculator.enrich_and_store_track_data(position_data, detection_data)
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
            vehicle_id=detection_data.get("vehicle_id"),
            queue_name="handler-3d",
            metadata={
                "positions_count": len(position_data.get("positions", [])),
                "data_type": "3d",
            },
        )

    logger.info(f"Processed 3D position calculation for vehicle {position_data.get('vehicle_id', 'unknown')}")
    return position_data
