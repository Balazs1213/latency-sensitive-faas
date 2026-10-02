from event import Event
import aio_pika
import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import math
import time
import redis

# Add monitoring module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'monitoring'))
sys.path.insert(0, '/app/monitoring')  # For Docker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

classes_to_detect = os.getenv("CLASSES_TO_DETECT", "person,pedestrian,car,truck,bus,motorcycle,bicycle").split(",")
CONFIDENCE = float(os.getenv("CONFIDENCE", "0.6"))

# Initialize monitoring client
try:
    from monitoring.tsdb_client import get_tsdb_client, extract_message_id
    tsdb = get_tsdb_client()
    MONITORING_ENABLED = tsdb.is_enabled()
    if MONITORING_ENABLED:
        logger.info("TSDB monitoring enabled")
    else:
        logger.warning("TSDB monitoring disabled or not configured")
except Exception as e:
    logger.warning(f"Failed to initialize TSDB monitoring: {e}")
    tsdb = None
    MONITORING_ENABLED = False
    extract_message_id = lambda x: None

# Configuration
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:30793/")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 32444))
FUSION_QUEUE = "detection-tracking-results"
THREED_DETECTION_TRACKING_QUEUE = "3d-tracking-results"
POSITION_QUEUE = "position-results"

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)
logger.info("Connected to Redis")


def calculate_distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance in meters between two GPS coordinates using Haversine formula.
    """
    R = 6371000  # Earth's radius in meters
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) *
         math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def calculate_heading(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate heading in degrees from point 1 to point 2.
    Returns heading in degrees (0-360).
    """
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    delta_lon = lon2_rad - lon1_rad
    y = math.sin(delta_lon) * math.cos(lat2_rad)
    x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon)
    bearing_rad = math.atan2(y, x)
    bearing_deg = math.degrees(bearing_rad)
    if bearing_deg < 0:
        bearing_deg += 360
    return bearing_deg


def calculate_heading_advanced(
    track_history: List[Dict[str, Any]],
    min_points: int = 3,
    max_points: int = 10,
    min_distance_meters: float = 1.0,
    relative_distance: float = 0
) -> float:
    """
    Calculate stable heading using advanced methods with full track history.
    Uses weighted least squares, distance-weighted averaging, circular statistics,
    and outlier rejection for noisy GPS data.
    Returns heading in degrees (0-360) or -1 if insufficient data.
    """
    if not track_history or len(track_history) < min_points:
        return -1

    positions = []
    track_history_reversed = list(reversed(track_history))
    for pos in track_history_reversed[:max_points]:
        if isinstance(pos, dict) and "position_3d" in pos:
            pos_3d = pos["position_3d"]
            if "lat" in pos_3d and "lon" in pos_3d:
                try:
                    lat = float(pos_3d["lat"])
                    lon = float(pos_3d["lon"])
                    positions.append({"lat": lat, "lon": lon})
                except (ValueError, TypeError):
                    continue

    if len(positions) < min_points:
        return -1

    positions = list(reversed(positions))
    segment_headings = []
    segment_distances = []
    segment_weights = []

    for i in range(len(positions) - 1):
        p1 = positions[i]
        p2 = positions[i + 1]
        distance = calculate_distance_meters(p1["lat"], p1["lon"], p2["lat"], p2["lon"])
        if distance < 0.1:
            continue
        heading = calculate_heading(p1["lat"], p1["lon"], p2["lat"], p2["lon"])
        segment_headings.append(heading)
        segment_distances.append(distance)
        recency_weight = math.exp((i - len(positions) + 1) * 0.2)
        segment_weights.append(distance * distance * recency_weight)

    if not segment_headings:
        return -1

    if relative_distance > 0:
        reference_distance = 10.0
        distance_ratio = relative_distance / reference_distance
        scale_factor = math.sqrt(distance_ratio)
        scale_factor = max(0.3, min(3.0, scale_factor))
        adjusted_min_distance = min_distance_meters * scale_factor
    else:
        adjusted_min_distance = min_distance_meters

    total_distance = sum(segment_distances)
    if total_distance < adjusted_min_distance:
        return -1

    weighted_x = 0.0
    weighted_y = 0.0
    total_weight = sum(segment_weights)
    for heading, weight in zip(segment_headings, segment_weights):
        heading_rad = math.radians(heading)
        weighted_x += weight * math.cos(heading_rad)
        weighted_y += weight * math.sin(heading_rad)

    if total_weight > 0:
        mean_x = weighted_x / total_weight
        mean_y = weighted_y / total_weight
        mean_heading_rad = math.atan2(mean_y, mean_x)
        mean_heading_deg = math.degrees(mean_heading_rad)
        if mean_heading_deg < 0:
            mean_heading_deg += 360

        circular_variance = 1.0 - math.sqrt(mean_x * mean_x + mean_y * mean_y)
        if circular_variance > 0.3:
            sorted_headings = sorted(segment_headings)
            median_idx = len(sorted_headings) // 2
            if len(sorted_headings) % 2 == 0:
                h1 = sorted_headings[median_idx - 1]
                h2 = sorted_headings[median_idx]
                h1_rad = math.radians(h1)
                h2_rad = math.radians(h2)
                mean_x = (math.cos(h1_rad) + math.cos(h2_rad)) / 2.0
                mean_y = (math.sin(h1_rad) + math.sin(h2_rad)) / 2.0
                mean_heading_rad = math.atan2(mean_y, mean_x)
                mean_heading_deg = math.degrees(mean_heading_rad)
                if mean_heading_deg < 0:
                    mean_heading_deg += 360
            else:
                mean_heading_deg = sorted_headings[median_idx]

        return mean_heading_deg

    return -1


async def get_from_redis(vehicle_id: str) -> Optional[Dict[str, Any]]:
    """Get vehicle data from Redis."""
    try:
        data = redis_client.get(f"vehicle:{vehicle_id}")
        if data is None:
            return None
        return json.loads(data)
    except Exception as e:
        logger.error(f"Error getting data from Redis for vehicle {vehicle_id}: {str(e)}")
        return None


async def store_in_redis(vehicle_id: str, vehicle_data: Dict[str, Any]) -> None:
    """Store vehicle data in Redis."""
    try:
        redis_client.set(
            f"vehicle:{vehicle_id}",
            json.dumps(vehicle_data)
        )
        logger.info(f"Updated Redis data for vehicle {vehicle_id}")
    except Exception as e:
        logger.error(f"Error storing data in Redis for vehicle {vehicle_id}: {str(e)}")


def enrich_positions_with_track_data(
    position_data: Dict[str, Any],
    vehicle_data: Dict[str, Any],
    base_min_distance: float,
    base_min_distance_person: float
) -> Dict[str, Any]:
    """
    Enrich positions with movement_heading, movement_distance, and is_moving
    based on track history. Updates vehicle_data in place.
    """
    positions = position_data.get("positions", [])
    track_ids = [p["track_id"] for p in positions if p.get("track_id")]

    vehicle_data["tracks"] = [
        t for t in vehicle_data.get("tracks", [])
        if t.get("track_id") in track_ids
    ]

    for position in positions:
        track_id = position.get("track_id")
        if not track_id:
            continue

        class_name = position.get("class", "unknown")
        if class_name == "person":
            min_distance = base_min_distance_person
        else:
            min_distance = base_min_distance

        track_added = False
        for track in vehicle_data["tracks"]:
            if track.get("track_id") == track_id:
                track["track_history"] = track.get("track_history", []) + [position]
                track["track_length"] = len(track["track_history"])
                if len(track["track_history"]) > 30:
                    track["track_history"].pop(0)
                track_added = True

                if len(track["track_history"]) >= 3:
                    prev_pos = track["track_history"][0 if len(track["track_history"]) < 5 else -5]
                    distance = 0.0
                    if "position_3d" in prev_pos and "position_3d" in position:
                        distance = calculate_distance_meters(
                            prev_pos["position_3d"]["lat"],
                            prev_pos["position_3d"]["lon"],
                            position["position_3d"]["lat"],
                            position["position_3d"]["lon"]
                        )
                        position.setdefault("position_3d", {})["movement_distance"] = distance

                    relative_distance = position.get("relative_position", {}).get("distance_m", 0)
                    if relative_distance > 0:
                        reference_distance = 10.0
                        distance_ratio = relative_distance / reference_distance
                        scale_factor = math.sqrt(distance_ratio)
                        scale_factor = max(0.3, min(3.0, scale_factor))
                        adjusted_min_distance = min_distance * scale_factor
                    else:
                        adjusted_min_distance = min_distance

                    heading = calculate_heading_advanced(
                        track["track_history"],
                        min_points=3,
                        max_points=min(15, len(track["track_history"])),
                        min_distance_meters=min_distance,
                        relative_distance=relative_distance
                    )

                    position.setdefault("position_3d", {})["movement_heading"] = heading
                    position["position_3d"]["is_moving"] = heading >= 0 and distance > adjusted_min_distance
                break

        if not track_added:
            vehicle_data["tracks"].append({
                "track_id": track_id,
                "track_history": [position],
                "track_length": 1
            })

    return position_data


class PositionCalculator:
    def __init__(self):
        self.rabbitmq_url = RABBITMQ_URL
        self.base_min_distance = float(os.getenv("MIN_DISTANCE_METERS", "0.5"))
        self.base_min_distance_person = float(os.getenv("MIN_DISTANCE_METERS_PERSON", "0.1"))

    async def enrich_and_store_track_data(
        self,
        position_data: Dict[str, Any],
        source_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enrich positions with movement_heading, movement_distance, is_moving
        using track history from Redis. Returns the enriched position_data.
        """
        vehicle_id = position_data.get("vehicle_id", "unknown")
        if not vehicle_id:
            return position_data

        vehicle_data = await get_from_redis(vehicle_id)
        if not vehicle_data:
            vehicle_data = {
                "vehicle_id": vehicle_id,
                "detections": [],
                "tracks": [],
                "positions": [],
                "last_update": datetime.now().isoformat(),
                "timestamp": position_data.get("timestamp"),
                "metadata": position_data.get("metadata", {})
            }

        vehicle_data["detections"] = source_data.get("detections", [])
        vehicle_data["positions"] = position_data.get("positions", [])
        vehicle_data["last_update"] = datetime.now().isoformat()
        vehicle_data["timestamp"] = position_data.get("timestamp", vehicle_data.get("timestamp"))
        vehicle_data.setdefault("metadata", {}).update(position_data.get("metadata", {}))

        enrich_positions_with_track_data(
            position_data,
            vehicle_data,
            self.base_min_distance,
            self.base_min_distance_person
        )

        position_data["tracks"] = vehicle_data.get("tracks", [])
        await store_in_redis(vehicle_id, vehicle_data)
        return position_data

    def calculate_2d_to_position(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate 3D position from 2D detection data.
        
        Args:
            fusion_data: Dictionary containing fusion results with detection information
            
        Returns:
            Dictionary containing calculated 3D positions
        """
        # TODO: Implement your position calculation logic here
        # This is a placeholder for the actual implementation
        detections = data.get("detections", [])
        detections = [detection for detection in detections if detection.get("confidence") >= CONFIDENCE and detection.get("class") in classes_to_detect]
        # Example structure of what you might calculate:
        positions = {
            "vehicle_id": data.get("vehicle_id", "unknown"),
            "timestamp": data.get("timestamp", datetime.now().isoformat()),
            "positions": [],
            "detections": detections,
            "metadata": data.get("metadata", {}),
            "tracks": data.get("tracks", [])
        }

        print("detections", len(detections))
        
        # Placeholder for position calculation
        # You would typically:
        # 1. Extract 2D bounding boxes from fusion_data
        # 2. Apply camera calibration parameters
        # 3. Use depth estimation or stereo vision
        # 4. Calculate 3D world coordinates
        # print("data",data)
        
        # Example placeholder calculation:
        if "detections" in positions:
            for detection in positions["detections"]:
                # Placeholder: convert 2D bbox to 3D position
                bbox = detection.get("bbox_2d", [0, 0, 0, 0])  # [x1, y1, x2, y2]
                if "relative_position" in detection:
                  detection["relative_position"]["direction_angle_rad"] = detection["relative_position"]["direction_angle_rad"]
                  detection["relative_position"]["direction_angle_deg"] = detection["relative_position"]["direction_angle_deg"]
                
                pos_relative = detection.get("relative_position", {})
                

                
              

                distance_m = pos_relative.get("distance_m", 0)

                # MAGIC!!!
                direction_angle_rad = pos_relative.get("direction_angle_rad", 0)

                distance_x_m = distance_m * math.sin(direction_angle_rad)
                distance_y_m = distance_m * math.cos(direction_angle_rad)

                # number of meters per degree in Wgs84 projection
                meters_per_degree = 111319.9
                position_lat = data["metadata"]["position"]["lat"] + distance_y_m / meters_per_degree
                position_lon = data["metadata"]["position"]["lon"] + distance_x_m / (meters_per_degree * math.cos(math.radians(data["metadata"]["position"]["lat"]))) 


                print("metadata", data["metadata"])

                print("direction_angle_rad", direction_angle_rad)
                print("direction_angle_deg", pos_relative.get("direction_angle_deg", 0))
                print("distance_m", distance_m)
                print("distance_x_m", distance_x_m)
                print("distance_y_m", distance_y_m)
                print("position_lat", position_lat)
                print("position_lon", position_lon)

                # TODO: Replace with actual position calculation
                calculated_position = {
                    **detection,
                    "position_3d": {
                        "lat": position_lat,
                        "lon": position_lon
                    },
                }
                positions["positions"].append(calculated_position)
        
        return positions

    def calculate_3d_to_position(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate position from 3D detection data.
        
        Args:
            data: Dictionary containing 3D detection and tracking results
            
        Returns:
            Dictionary containing calculated positions with 3D data
        """

        # print("data", data)
        positions = {
            "vehicle_id": data.get("vehicle_id", "unknown"),
            "timestamp": data.get("timestamp", datetime.now().isoformat()),
            "positions": [],
            "detections": data.get("detections", []),
            "metadata": data.get("metadata", {}),
            "tracks": data.get("tracks", []),
            "processed_at": data.get("processed_at", datetime.now().isoformat())
        }
        
        logger.info(f"Processing 3D detection data for vehicle {data.get('vehicle_id', 'unknown')}")
        track_2dbox_to_track_id = {}
        if "tracks" in data:
            for track in data["tracks"]:
              if "track_id" in track and "bbox_2d" in track:
                bbox_2d = track["bbox_2d"]
                track_2dbox_to_track_id[f"{bbox_2d[0]},{bbox_2d[1]},{bbox_2d[2]},{bbox_2d[3]}"] = track["track_id"]
        
        # Process 3D detections
        if "detections" in data:
            for detection in data["detections"]:
                if "track_id" not in detection:
                  track_id = track_2dbox_to_track_id.get(f"{detection.get('bbox_2d', [0, 0, 0, 0])[0]},{detection.get('bbox_2d', [0, 0, 0, 0])[1]},{detection.get('bbox_2d', [0, 0, 0, 0])[2]},{detection.get('bbox_2d', [0, 0, 0, 0])[3]}", None)
                  detection["track_id"] = track_id
                # Extract 3D location data
                location_3d = detection.get("location", [0, 0, 0])  # [x, y, z]
                dimensions = detection.get("dimensions", [0, 0, 0])  # [length, width, height]
                bbox_3d = detection.get("bbox_3d", [0, 0, 0, 0, 0, 0, 0])  # [x, y, z, l, w, h, rotation]
                rotation_y = detection.get("rotation_y", 0)

                detection.update({
                  "relative_position": {
                    "distance_m": math.sqrt(location_3d[0]**2 + location_3d[2]**2),
                    "direction_angle_rad": rotation_y,
                    "direction_angle_deg": rotation_y * 180 / math.pi
                  }
                })
                
                # Convert 3D coordinates to GPS coordinates
                # Assuming the 3D coordinates are relative to the vehicle's position
                if "position" in data["metadata"]:
                  vehicle_lat = data["metadata"]["position"]["lat"]
                  vehicle_lon = data["metadata"]["position"]["lon"]
                else:
                  vehicle_lat = 47.4729586
                  vehicle_lon = 19.0571794
                
                # Convert 3D position to GPS coordinates
                # Using the same conversion logic as 2D but with 3D coordinates
                meters_per_degree = 111319.9
                
                # For 3D data, we use the x and z coordinates (forward/backward and left/right)
                # y is typically height, which we don't use for GPS conversion
                position_lat = vehicle_lat + location_3d[0] / meters_per_degree  # x -> lat
                position_lon = vehicle_lon + location_3d[2] / meters_per_degree  # z -> lon
                
                calculated_position = {
                    **detection,
                    "position_3d": {
                        "lat": position_lat,
                        "lon": position_lon,
                        "altitude": location_3d[1]  # y coordinate as altitude
                    },
                    "gps_position": {
                        "lat": position_lat,
                        "lon": position_lon,
                        "altitude": location_3d[1]
                    }
                }
                positions["positions"].append(calculated_position)
        
        # Process 3D tracks
        if "tracks" in data:
            for track in data["tracks"]:
                # Extract 3D location data from tracks
                location_3d = track.get("location", [0, 0, 0])
                dimensions = track.get("dimensions", [0, 0, 0])
                rotation_y = track.get("rotation_y", 0)
                
                # Convert 3D coordinates to GPS coordinates
                if "position" in data["metadata"]:
                    vehicle_lat = data["metadata"]["position"]["lat"]
                    vehicle_lon = data["metadata"]["position"]["lon"]
                else:
                    vehicle_lat = 47.4729586
                    vehicle_lon = 19.0571794
                
                meters_per_degree = 111319.9
                position_lat = vehicle_lat + location_3d[0] / meters_per_degree
                position_lon = vehicle_lon + location_3d[2] / meters_per_degree
                
                # Add GPS position to track data
                track["position_3d"] = {
                    "lat": position_lat,
                    "lon": position_lon,
                    "altitude": location_3d[1]
                }
                track["gps_position"] = {
                    "lat": position_lat,
                    "lon": position_lon,
                    "altitude": location_3d[1]
                }
        
        return positions

    async def publish_position_result(self, position_data: Dict[str, Any]):
        """Publish position calculation result to RabbitMQ."""
        try:
            # Create connection
            connection = await aio_pika.connect_robust(self.rabbitmq_url)
            
            async with connection:
                # Create channel
                channel = await connection.channel()
                
                # Declare queue
                queue = await channel.declare_queue(
                    POSITION_QUEUE,
                    durable=True,
                    arguments={
                        "x-message-ttl": int(os.getenv('MESSAGE_TTL', 1000)),
                        "x-max-length": int(os.getenv('MAX_LENGTH', 10))
                    }
                )
                
                # Publish message
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(position_data).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key=POSITION_QUEUE
                )
                # print("position_data",position_data)
                
                logger.info(f"Published position result for vehicle {position_data.get('vehicle_id', 'unknown')}")
                
        except Exception as e:
            logger.error(f"Error publishing position result: {str(e)}")

    async def process_2d_message(self, message: aio_pika.IncomingMessage):
        """Process incoming 2D fusion message from RabbitMQ."""
        async with message.process():
            message_id = None
            fusion_data = None
            
            try:
                # Parse the message
                fusion_data = json.loads(message.body.decode())
                
                # Extract or generate message_id
                if MONITORING_ENABLED and tsdb:
                    message_id = extract_message_id(fusion_data)
                
                logger.info(f"Received 2D fusion data for processing: {fusion_data.get('vehicle_id', 'unknown')}")
                
                # Track processing
                if MONITORING_ENABLED and tsdb and message_id:
                        start_time = time.time()
                        # Calculate positions from 2D data
                        position_data = self.calculate_2d_to_position(fusion_data)
                        # Enrich with track history (heading, movement, is_moving)
                        position_data = await self.enrich_and_store_track_data(position_data, fusion_data)
                        
                        # Add message_id to result
                        if message_id:
                            position_data["message_id"] = message_id
                        
                        # Record additional metadata
                        if MONITORING_ENABLED and tsdb:
                            tsdb.record_message_processing(
                                message_id=message_id,
                                service_name="position-service",
                                processing_time_ms=int((time.time() - start_time) * 1000),
                                status="success",
                                vehicle_id=fusion_data.get("vehicle_id"),
                                queue_name=FUSION_QUEUE,
                                metadata={
                                    "positions_count": len(position_data.get("positions", [])),
                                    "data_type": "2d"
                                }
                            )
                        
                        # Publish position results
                        await self.publish_position_result(position_data)
                        logger.info(f"Processed 2D position calculation for vehicle {position_data.get('vehicle_id', 'unknown')}")
                else:
                    # Fallback: process without monitoring
                    position_data = self.calculate_2d_to_position(fusion_data)
                    position_data = await self.enrich_and_store_track_data(position_data, fusion_data)
                    if message_id:
                        position_data["message_id"] = message_id
                    await self.publish_position_result(position_data)
                    logger.info(f"Processed 2D position calculation for vehicle {position_data.get('vehicle_id', 'unknown')}")
                
            except Exception as e:
                # Record error
                if MONITORING_ENABLED and tsdb and message_id:
                    tsdb.record_message_processing(
                        message_id=message_id,
                        service_name="position-service",
                        processing_time_ms=0,
                        status="error",
                        vehicle_id=fusion_data.get("vehicle_id") if fusion_data else None,
                        queue_name=FUSION_QUEUE,
                        metadata={"error": str(e), "data_type": "2d"}
                    )
                logger.error(f"Error processing 2D fusion message: {str(e)}")

    async def process_3d_message(self, message: aio_pika.IncomingMessage):
        """Process incoming 3D detection message from RabbitMQ."""
        async with message.process():
            message_id = None
            detection_data = None
            
            try:
                # Parse the message
                detection_data = json.loads(message.body.decode())
                
                # Extract or generate message_id
                if MONITORING_ENABLED and tsdb:
                    message_id = extract_message_id(detection_data)
                
                logger.info(f"Received 3D detection data for processing: {detection_data.get('vehicle_id', 'unknown')}")
                
                # Track processing
                if MONITORING_ENABLED and tsdb and message_id:
                        start_time = time.time()
                        # Calculate positions from 3D data
                        position_data = self.calculate_3d_to_position(detection_data)
                        # Enrich with track history (heading, movement, is_moving)
                        position_data = await self.enrich_and_store_track_data(position_data, detection_data)
                        
                        # Add message_id to result
                        if message_id:
                            position_data["message_id"] = message_id
                        
                        # Record additional metadata
                        if MONITORING_ENABLED and tsdb:
                            tsdb.record_message_processing(
                                message_id=message_id,
                                service_name="position-service",
                                processing_time_ms=int((time.time() - start_time) * 1000),
                                status="success",
                                vehicle_id=detection_data.get("vehicle_id"),
                                queue_name=THREED_DETECTION_TRACKING_QUEUE,
                                metadata={
                                    "positions_count": len(position_data.get("positions", [])),
                                    "data_type": "3d"
                                }
                            )
                        
                        # Publish position results
                        await self.publish_position_result(position_data)
                        logger.info(f"Processed 3D position calculation for vehicle {position_data.get('vehicle_id', 'unknown')}")
                else:
                    # Fallback: process without monitoring
                    position_data = self.calculate_3d_to_position(detection_data)
                    position_data = await self.enrich_and_store_track_data(position_data, detection_data)
                    if message_id:
                        position_data["message_id"] = message_id
                    await self.publish_position_result(position_data)
                    logger.info(f"Processed 3D position calculation for vehicle {position_data.get('vehicle_id', 'unknown')}")
                
            except Exception as e:
                # Record error
                if MONITORING_ENABLED and tsdb and message_id:
                    tsdb.record_message_processing(
                        message_id=message_id,
                        service_name="position-service",
                        processing_time_ms=0,
                        status="error",
                        vehicle_id=detection_data.get("vehicle_id") if detection_data else None,
                        queue_name=THREED_DETECTION_TRACKING_QUEUE,
                        metadata={"error": str(e), "data_type": "3d"}
                    )
                logger.error(f"Error processing 3D detection message: {str(e)}")

    async def start(self):
        """Start the position calculation service."""
        try:
            # Create RabbitMQ connection
            connection = await aio_pika.connect_robust(self.rabbitmq_url)
            
            async with connection:
                # Create channel
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=1)
                # Declare both queues to consume from
                fusion_queue = await channel.get_queue(FUSION_QUEUE)
                detection_3d_queue = await channel.get_queue(THREED_DETECTION_TRACKING_QUEUE)
                
                # Start consuming from both queues
                await fusion_queue.consume(self.process_2d_message)
                await detection_3d_queue.consume(self.process_3d_message, arguments = {
                  "x-stream-offset": "next"
                })
                
                
                logger.info(f"Started consuming from 2D fusion queue: {FUSION_QUEUE}")
                logger.info(f"Started consuming from 3D detection queue: {THREED_DETECTION_TRACKING_QUEUE}")
                logger.info(f"Position results will be published to: {POSITION_QUEUE}")
                
                # Keep the consumer running
                await asyncio.Future()
                
        except Exception as e:
            logger.error(f"Error in position calculator: {str(e)}")
            # Wait before retrying
            await asyncio.sleep(5)
            await self.start()

if __name__ == "__main__":
    logger.info("Starting Position Calculator (2D and 3D)")
    calculator = PositionCalculator()
    asyncio.run(calculator.start())


# --- Handlers for Egon's FaaS framework (position-ru-fusion composed function) ---
# These wrap the pure calculation + Redis-enrichment logic above without the
# RabbitMQ message/consumer wrapping, so they can be registered in config.py's
# HANDLERS dict and invoked directly by func.py.

_position_calculator = PositionCalculator()


def position_service_2d_handler(event: Event) -> Dict[str, Any]:
    """Position handler for Egon's FaaS framework - contains the logic of
    process_2d_message(), without the RabbitMQ message wrapping."""
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


def position_service_3d_handler(event: Event) -> Dict[str, Any]:
    """Position handler for Egon's FaaS framework - contains the logic of
    process_3d_message(), without the RabbitMQ message wrapping."""
    detection_data = event.json or {}

    message_id = None
    if MONITORING_ENABLED and tsdb:
        message_id = extract_message_id(detection_data)

    logger.info(f"Received 3D detection data for processing: {detection_data.get('vehicle_id', 'unknown')}")
    start_time = time.time()

    # Calculate positions from 3D data
    position_data = _position_calculator.calculate_3d_to_position(detection_data)

    # Enrich with track history (heading, movement, is_moving) - see the note
    # in position_service_2d_handler about asyncio.run() above.
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
