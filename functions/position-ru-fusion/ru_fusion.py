from event import Event
import aio_pika
import asyncio
import json
import logging
import os
import sys
from typing import List, Dict, Optional, Any, Tuple
import redis
from datetime import datetime
import math
import time
import uuid

# Add monitoring module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'monitoring'))
sys.path.insert(0, '/app/monitoring')  # For Docker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
REDIS_HOST = os.getenv("REDIS_HOST", "redis.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
POSITION_QUEUE = "position-results"
FUSION_QUEUE = "fusion-results-stream"

# Fusion configuration
GATING_RADIUS_M = float(os.getenv("GATING_RADIUS_M", "5.0"))
GATING_RADIUS_M_PERSON = float(os.getenv("GATING_RADIUS_M_PERSON", "1.0"))
ASSOCIATION_THRESHOLD = float(os.getenv("ASSOCIATION_THRESHOLD", "15.0"))
FUSED_OBJECT_TTL_S = float(os.getenv("FUSED_OBJECT_TTL_S", "2.0"))
EGO_TTL_S = float(os.getenv("EGO_TTL_S", "15.0"))

# Redis key prefixes
FUSED_GEO_KEY = "fused:geo"
FUSED_OBJ_PREFIX = "fused:obj:"
EGO_GEO_KEY = "fused:egos"
EGO_PREFIX = "fused:ego:"

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)
logger.info("Connected to Redis")

class FusionRequest:
    def __init__(self, vehicle_id: str, timestamp: float, detections: List[Dict[str, Any]] = None, 
                 tracks: List[Dict[str, Any]] = None, positions: List[Dict[str, Any]] = None, routing_key: str = "", metadata: Dict[str, Any] = None):
        self.vehicle_id = vehicle_id
        self.timestamp = timestamp
        self.detections = detections or []
        self.tracks = tracks or []
        self.routing_key = routing_key
        self.metadata = metadata or {}
        self.positions = positions or []

class RoadUser:
    def __init__(self, id: str, class_type: str, position: List[float], 
                 velocity: List[float], direction: float, confidence: float, 
                 track_history: List[List[float]]):
        self.id = id
        self.class_type = class_type
        self.position = position
        self.velocity = velocity
        self.direction = direction
        self.confidence = confidence
        self.track_history = track_history

class FusionResult:
    def __init__(self, vehicle_id: str, timestamp: float, detections: List[Dict[str, Any]], 
                 tracks: List[Dict[str, Any]], positions: List[Dict[str, Any]]):
        self.vehicle_id = vehicle_id
        self.timestamp = timestamp
        self.detections = detections
        self.tracks = tracks
        self.positions = positions

import math

def calculate_distance_meters(lat1, lon1, lat2, lon2):
    """
    Calculate distance in meters between two GPS coordinates using Haversine formula.
    
    Args:
        lat1, lon1: Latitude and longitude of point 1 (in degrees)
        lat2, lon2: Latitude and longitude of point 2 (in degrees)
    
    Returns:
        Distance in meters
    """
    # Earth's radius in meters
    R = 6371000
    
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Calculate differences
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    
    # Haversine formula
    a = (math.sin(delta_lat / 2) ** 2 + 
         math.cos(lat1_rad) * math.cos(lat2_rad) * 
         math.sin(delta_lon / 2) ** 2)
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Calculate distance
    distance = R * c
    
    return distance

def calculate_heading(lat1, lon1, lat2, lon2):
    """
    Calculate heading in degrees from point 1 to point 2.
    This is a simple two-point calculation for backward compatibility.
    
    Args:
        lat1, lon1: Latitude and longitude of point 1 (in degrees)
        lat2, lon2: Latitude and longitude of point 2 (in degrees)
    
    Returns:
        Heading in degrees (0-360)
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Calculate differences
    delta_lon = lon2_rad - lon1_rad
    
    # Calculate bearing using spherical law of cosines
    y = math.sin(delta_lon) * math.cos(lat2_rad)
    x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon)
    
    bearing_rad = math.atan2(y, x)
    bearing_deg = math.degrees(bearing_rad)
    
    # Normalize to 0-360
    if bearing_deg < 0:
        bearing_deg += 360
    
    return bearing_deg

def calculate_heading_advanced(track_history: List[Dict[str, Any]], 
                                min_points: int = 3, 
                                max_points: int = 10,
                                min_distance_meters: float = 1.0, relative_distance: float = 0) -> float:
    """
    Calculate stable heading using advanced methods with full track history.
    
    This function uses multiple techniques for stability:
    1. Weighted least squares regression through multiple points
    2. Distance-weighted averaging (longer segments are more reliable)
    3. Circular statistics for proper heading averaging
    4. Outlier rejection for noisy GPS data
    5. Relative distance scaling: adjusts movement threshold based on object distance from observer
    
    Args:
        track_history: List of position dictionaries with 'position_3d' containing 'lat' and 'lon'
        min_points: Minimum number of points required (default: 3)
        max_points: Maximum number of recent points to use (default: 10)
        min_distance_meters: Base minimum total distance required for reliable heading (default: 1.0m).
                             This is scaled based on relative_distance - closer objects need less movement,
                             farther objects need more movement due to detection accuracy.
        relative_distance: Distance in meters between the object and the observer. Used to adjust
                          min_distance_meters threshold. If 0 or not provided, base threshold is used.
    
    Returns:
        Heading in degrees (0-360) or -1 if insufficient data or object is not moving
    """
    if not track_history or len(track_history) < min_points:
        return -1
    
    # Extract valid positions with lat/lon
    positions = []
    track_history_reversed = list(reversed(track_history))
    for pos in track_history_reversed[:max_points]:  # Use most recent points
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
    
    # Reverse to get chronological order (oldest first)
    positions = list(reversed(positions))
    
    # Calculate segment headings and distances
    segment_headings = []
    segment_distances = []
    segment_weights = []
    
    for i in range(len(positions) - 1):
        p1 = positions[i]
        p2 = positions[i + 1]
        
        # Calculate distance for this segment
        distance = calculate_distance_meters(
            p1["lat"], p1["lon"],
            p2["lat"], p2["lon"]
        )
        
        # Skip very short segments (likely GPS noise)
        if distance < 0.1:  # Less than 10cm
            continue
        
        # Calculate heading for this segment
        heading = calculate_heading(
            p1["lat"], p1["lon"],
            p2["lat"], p2["lon"]
        )
        
        segment_headings.append(heading)
        segment_distances.append(distance)
        
        # Weight by distance squared (longer segments are more reliable)
        # Also apply exponential decay for recency (more recent = higher weight)
        recency_weight = math.exp((i - len(positions) + 1) * 0.2)
        segment_weights.append(distance * distance * recency_weight)
    
    if not segment_headings:
        return -1
    
    # Adjust min_distance_meters based on relative_distance from observer
    # Objects closer to observer (more accurate detection) need less movement
    # Objects farther away (less accurate detection) need more movement
    if relative_distance > 0:
        # Reference distance where base threshold applies (e.g., 10m)
        reference_distance = 10.0  # meters
        
        # Scale factor: use square root for smoother scaling
        # Closer objects (< reference): threshold decreases
        # Farther objects (> reference): threshold increases
        # Formula: adjusted = base * sqrt(relative_distance / reference)
        # This gives: 5m -> 0.71x, 10m -> 1.0x, 20m -> 1.41x, 50m -> 2.24x
        distance_ratio = relative_distance / reference_distance
        scale_factor = math.sqrt(distance_ratio)
        
        # Apply bounds to prevent extreme values
        # Minimum scale: 0.3x (for very close objects, e.g., 1m)
        # Maximum scale: 3.0x (for very far objects, e.g., 90m)
        scale_factor = max(0.3, min(3.0, scale_factor))
        
        adjusted_min_distance = min_distance_meters * scale_factor
    else:
        # If relative_distance not provided, use base threshold
        adjusted_min_distance = min_distance_meters
    
    # Check total distance - if object hasn't moved enough, return -1
    total_distance = sum(segment_distances)
    if total_distance < adjusted_min_distance:
        return -1
    
    # Method 1: Weighted circular mean (most stable for noisy data)
    # Convert headings to unit vectors, weight them, then convert back
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
        
        # Calculate mean heading from weighted vector
        mean_heading_rad = math.atan2(mean_y, mean_x)
        mean_heading_deg = math.degrees(mean_heading_rad)
        
        # Normalize to 0-360
        if mean_heading_deg < 0:
            mean_heading_deg += 360
        
        # Method 2: Outlier rejection using circular statistics
        # Calculate circular variance to detect outliers
        circular_variance = 1.0 - math.sqrt(mean_x * mean_x + mean_y * mean_y)
        
        # If variance is high, use median instead (more robust)
        if circular_variance > 0.3:  # High variance threshold
            # Use median of recent headings (more robust to outliers)
            sorted_headings = sorted(segment_headings)
            median_idx = len(sorted_headings) // 2
            if len(sorted_headings) % 2 == 0:
                # Even number: average two middle values using circular mean
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

async def process_position_data(data: FusionRequest):
    """Process detection data and update vehicle data."""
    vehicle_id = data.vehicle_id
    if not vehicle_id:
        logger.warning("Received detection data without vehicle_id")
        return None
    
    vehicle_data = await get_from_redis(vehicle_id)

    if not vehicle_data:
        vehicle_data = {
            "vehicle_id": vehicle_id,
            "detections": [],
            "tracks": [],
            "positions": [],
            "last_update": datetime.now().isoformat(),
            "timestamp": data.timestamp,
            "metadata": data.metadata or {}
        }

    vehicle_data["detections"] = data.detections
    vehicle_data["last_update"] = datetime.now().isoformat()
    vehicle_data["timestamp"] = data.timestamp
    vehicle_data["positions"] = data.positions
    vehicle_data["metadata"] = vehicle_data.get("metadata", {})
    vehicle_data["metadata"].update(data.metadata or {})

    # extend the positions with the heading angle based on the track
    # print(data.tracks)
    # print(data.positions)
    track_ids = [position["track_id"] for position in data.positions if "track_id" in position and position["track_id"]]
    vehicle_data["tracks"] = [track for track in vehicle_data["tracks"] if "track_id" in track and track["track_id"] in track_ids]

    for position in data.positions:
        track_id = position.get("track_id", None)
        if track_id:
            track_added = False
            for track in vehicle_data["tracks"]:   
                if "track_id" in track and track["track_id"] == track_id:
                    track.update({"track_history": track["track_history"] + [position]})
                    track.update({"track_length": len(track["track_history"])})
                    if len(track["track_history"]) > 30:
                        track["track_history"].pop(0)
                    track_added = True
                    # calculate heading based on the GPS position using advanced method
                    if len(track["track_history"]) >= 3:
                        # Calculate distance from previous position
                        prev_pos = track["track_history"][0 if len(track["track_history"]) < 5 else -5]  # Most recent previous position
                        distance = 0
                        if "position_3d" in prev_pos and "position_3d" in position:
                            distance = calculate_distance_meters(
                                prev_pos["position_3d"]["lat"], 
                                prev_pos["position_3d"]["lon"], 
                                position["position_3d"]["lat"], 
                                position["position_3d"]["lon"]
                            )
                            position["position_3d"].update({"movement_distance": distance})
                        
                        # Use advanced heading calculation with full track history
                        relative_distance = position.get("relative_position", {}).get("distance_m", 0)
                        base_min_distance = float(os.getenv("MIN_DISTANCE_METERS", 0.5))
                        
                        # Calculate adjusted threshold based on relative distance
                        if relative_distance > 0:
                            reference_distance = 10.0
                            distance_ratio = relative_distance / reference_distance
                            scale_factor = math.sqrt(distance_ratio)
                            scale_factor = max(0.3, min(3.0, scale_factor))
                            adjusted_min_distance = base_min_distance * scale_factor
                        else:
                            adjusted_min_distance = base_min_distance
                        
                        heading = calculate_heading_advanced(
                            track["track_history"],
                            min_points=3,
                            max_points=min(15, len(track["track_history"])),  # Use up to 15 most recent points
                            min_distance_meters=base_min_distance,  # Base threshold (scaled internally by relative_distance)
                            relative_distance=relative_distance
                        )
                        position["position_3d"].update({"movement_heading": heading})
                        # Use adjusted threshold for is_moving check for consistency
                        position["position_3d"].update({"is_moving": True if heading >= 0 and distance > adjusted_min_distance else False})
                    break
            if not track_added:
              vehicle_data["tracks"].append({
                  "track_id": track_id,
                  "track_history": [position],
                  "track_length": 1
              })
              print(vehicle_data["tracks"])
                    

        


    
    # Store in Redis
    await store_in_redis(vehicle_id, vehicle_data)
    
    return vehicle_data

def is_overlap(bbox1: List[float], bbox2: List[float]) -> bool:
    """Check if two bounding boxes overlap."""
    required_overlap_ratio = 0.9
    x1, y1, w1, h1 = bbox1
    x2, y2, w2, h2 = bbox2
    # calculate the overlap ratio
    x1_overlap = max(x1, x2)
    y1_overlap = max(y1, y2)
    x2_overlap = min(x1 + w1, x2 + w2)
    y2_overlap = min(y1 + h1, y2 + h2)
    overlap_area = max(0, x2_overlap - x1_overlap) * max(0, y2_overlap - y1_overlap)
    area1 = w1 * h1
    area2 = w2 * h2
    overlap_ratio = overlap_area / (area1 + area2 - overlap_area)
    return overlap_ratio >= required_overlap_ratio

async def get_from_redis(vehicle_id: str):
    """Get vehicle data from Redis."""
    logger.info(f"Getting data from Redis for vehicle {vehicle_id}")
    try:
        data = redis_client.get(f"vehicle:{vehicle_id}")
        if data is None:
            return None
        return json.loads(data)
    except Exception as e:
        logger.error(f"Error getting data from Redis for vehicle {vehicle_id}: {str(e)}")
        return None

async def store_in_redis(vehicle_id: str, vehicle_data: Dict[str, Any]):
    """Store vehicle data in Redis."""
    try:
        redis_client.set(
            f"vehicle:{vehicle_id}",
            json.dumps(vehicle_data)
        )
        logger.info(f"Updated Redis data for vehicle {vehicle_id}")
    except Exception as e:
        logger.error(f"Error storing data in Redis for vehicle {vehicle_id}: {str(e)}")

async def publish_fusion_result(data: Dict[str, Any]):
    """Publish fusion result to RabbitMQ."""
    try:
        # Create connection
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        
        async with connection:
            # Create channel
            channel = await connection.channel()
            
            # Declare queue
            # queue = await channel.declare_queue(
            #     FUSION_QUEUE,
            #     durable=True,
            #     arguments={
            #         "x-message-ttl": int(os.getenv('MESSAGE_TTL', 1000)),
            #         "x-max-length": int(os.getenv('MAX_LENGTH', 10))
            #     }
            # )
            await channel.declare_queue(
                FUSION_QUEUE,
                durable=True,
                arguments={
                    "x-queue-type": "stream", 
                    "x-max-age": f"{int(os.getenv('MESSAGE_TTL', 10))}s",
                    # "x-max-length": int(os.getenv('MAX_LENGTH', 10))
                }
            )
            
            # Publish message
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"x-stream-filter-value": data.get("vehicle_id", "")}
                ),
                routing_key=FUSION_QUEUE,

            )
            
            logger.info(f"Published fusion result for vehicle {data['vehicle_id']}")
            
    except Exception as e:
        logger.error(f"Error publishing fusion result: {str(e)}")
            

async def process_message(message: aio_pika.IncomingMessage):
    """Process incoming message from RabbitMQ."""
    async with message.process():
        message_id = None
        data = None
        queue_name = message.routing_key or "unknown"
        
        try:
            # Parse the message
            data = json.loads(message.body.decode())
            data["routing_key"] = message.routing_key
            
            # Extract or generate message_id
            if MONITORING_ENABLED and tsdb:
                message_id = extract_message_id(data)
            
            logger.info(f"Processing message for vehicle {data.get('vehicle_id', 'unknown')}")
            
            # Create FusionRequest object
            request = FusionRequest(
                vehicle_id=data.get("vehicle_id", ""),
                timestamp=data.get("timestamp", 0),
                detections=data.get("detections", []),
                positions=data.get("positions", []),
                tracks=data.get("tracks", []),
                routing_key=data.get("routing_key", ""),
                metadata=data.get("metadata", {})
            )
            
            # Track processing
            if MONITORING_ENABLED and tsdb and message_id:
                    start_time = time.time()
                    # Process based on queue
                    vehicle_data = None
                    if request.routing_key == POSITION_QUEUE:
                        vehicle_data = await process_position_data(request)
                    
                    if vehicle_data:
                        # Add message_id to result
                        if message_id:
                            vehicle_data["message_id"] = message_id
                        
                        # Record additional metadata
                        if MONITORING_ENABLED and tsdb:
                            tsdb.record_message_processing(
                                message_id=message_id,
                                service_name="ru-fusion",
                                processing_time_ms=int((time.time() - start_time) * 1000),
                                status="success",
                                vehicle_id=data.get("vehicle_id"),
                                queue_name=queue_name,
                                metadata={
                                    "fused_objects_count": len(vehicle_data.get("positions", [])),
                                    "tracks_count": len(vehicle_data.get("tracks", []))
                                }
                            )
                        
                        # Publish fusion result
                        await publish_fusion_result(vehicle_data)
            else:
                # Fallback: process without monitoring
                vehicle_data = None
                if request.routing_key == POSITION_QUEUE:
                    vehicle_data = await process_position_data(request)
                
                if vehicle_data:
                    if message_id:
                        vehicle_data["message_id"] = message_id
                    await publish_fusion_result(vehicle_data)
            
        except Exception as e:
            # Record error
            if MONITORING_ENABLED and tsdb and message_id:
                tsdb.record_message_processing(
                    message_id=message_id,
                    service_name="ru-fusion",
                    processing_time_ms=0,
                    status="error",
                    vehicle_id=data.get("vehicle_id") if data else None,
                    queue_name=queue_name,
                    metadata={"error": str(e)}
                )
            logger.error(f"Error processing message: {str(e)}")

def circular_heading_diff(h1: float, h2: float) -> float:
    """Absolute angular difference between two headings (0-180)."""
    if h1 < 0 or h2 < 0:
        return 0.0
    diff = abs(h1 - h2) % 360
    return diff if diff <= 180 else 360 - diff


def association_cost(
    obs_lat: float, obs_lon: float, obs_class: str,
    obs_heading: float, obs_is_moving: bool,
    cand_lat: float, cand_lon: float, cand_class: str,
    cand_heading: float, cand_is_moving: bool
) -> float:
    """
    Compute association cost between an observation and a candidate.
    Returns float('inf') if class mismatch or distance exceeds gating radius.
    """
    dist = calculate_distance_meters(obs_lat, obs_lon, cand_lat, cand_lon)
    if dist > GATING_RADIUS_M:
        return float('inf')
    if obs_class != cand_class:
        return float('inf')

    heading_diff = circular_heading_diff(obs_heading, cand_heading)
    heading_penalty = (heading_diff / 180.0) * 2.0

    moving_penalty = 0.0
    if obs_is_moving != cand_is_moving:
        moving_penalty = 3.0

    return dist + heading_penalty + moving_penalty


def geo_search_candidates(geo_key: str, lon: float, lat: float, radius_m: float) -> List[Tuple[str, float]]:
    """
    Search Redis GEO index for members within radius_m of (lon, lat).
    Returns list of (member_id, distance_m).
    """
    results = redis_client.geosearch(
        geo_key,
        longitude=lon,
        latitude=lat,
        radius=radius_m,
        unit="m",
        withdist=True,
        sort="ASC"
    )
    return [(member, dist) for member, dist in results]


def get_fused_object(fused_id: str) -> Optional[Dict[str, Any]]:
    data = redis_client.get(f"{FUSED_OBJ_PREFIX}{fused_id}")
    if data is None:
        return None
    return json.loads(data)


def save_fused_object(fused_obj: Dict[str, Any]) -> None:
    fused_id = fused_obj["fused_id"]
    redis_client.set(
        f"{FUSED_OBJ_PREFIX}{fused_id}",
        json.dumps(fused_obj)
    )
    redis_client.geoadd(
        FUSED_GEO_KEY,
        (fused_obj["position"]["lon"], fused_obj["position"]["lat"], fused_id)
    )


def remove_fused_object(fused_id: str) -> None:
    redis_client.delete(f"{FUSED_OBJ_PREFIX}{fused_id}")
    redis_client.zrem(FUSED_GEO_KEY, fused_id)


def get_ego(vehicle_id: str) -> Optional[Dict[str, Any]]:
    data = redis_client.get(f"{EGO_PREFIX}{vehicle_id}")
    if data is None:
        return None
    return json.loads(data)


def save_ego(ego: Dict[str, Any]) -> None:
    vehicle_id = ego["vehicle_id"]
    redis_client.set(f"{EGO_PREFIX}{vehicle_id}", json.dumps(ego))
    redis_client.geoadd(
        EGO_GEO_KEY,
        (ego["position"]["lon"], ego["position"]["lat"], vehicle_id)
    )


def upsert_ego_vehicle(vehicle_id: str, metadata: Dict[str, Any], timestamp: str) -> None:
    """Register or update an ego vehicle (active participant)."""
    position = metadata.get("position", {})
    heading = metadata.get("heading", -1)
    ego = {
        "vehicle_id": vehicle_id,
        "type": "active_ego",
        "position": {"lat": position.get("lat", 0), "lon": position.get("lon", 0)},
        "heading": heading,
        "last_update": timestamp,
        "metadata": metadata,
    }
    save_ego(ego)


# How strongly each new observation pulls the fused position.
# Keep this low (0.05–0.20) so GPS noise from misaligned sources cannot
# cause the fused position to oscillate.  Scaled further by the observation
# confidence at runtime.
POSITION_EMA_ALPHA = float(os.getenv("POSITION_EMA_ALPHA", 0.12))


def merge_observation_into_fused(
    fused_obj: Dict[str, Any],
    obs: Dict[str, Any],
    source_vehicle_id: str,
    timestamp: str
) -> Dict[str, Any]:
    """
    Merge an observation into an existing fused object using an
    exponential moving average (EMA).

    The fused position is treated as the ground truth and only nudged
    slightly toward the new observation, preventing GPS-offset oscillation
    when two sources report the same target with slightly different
    coordinates.

    Effective alpha = POSITION_EMA_ALPHA * obs_confidence, so a low-
    confidence report barely moves the fused position at all.
    """
    observers = fused_obj.get("observers", {})

    obs_confidence = obs.get("confidence", 0.5)
    obs_lat = obs["position_3d"]["lat"]
    obs_lon = obs["position_3d"]["lon"]
    obs_heading = obs.get("position_3d", {}).get("movement_heading", -1)
    obs_is_moving = obs.get("position_3d", {}).get("is_moving", False)

    # Preserve the last known bbox for this observer if the new message doesn't provide one.
    # Upstream (2d-detect-and-track) emits bbox_2d as a list: [x1, y1, x2, y2]
    prev_bbox = observers.get(source_vehicle_id, {}).get("bbox_2d")
    new_bbox = obs.get("bbox_2d")
    if isinstance(new_bbox, list) and len(new_bbox) == 4:
        bbox_to_store = new_bbox
    else:
        bbox_to_store = prev_bbox if isinstance(prev_bbox, list) and len(prev_bbox) == 4 else []

    observers[source_vehicle_id] = {
        "track_id": obs.get("track_id"),
        "last_seen": timestamp,
        "confidence": obs_confidence,
        "position": {"lat": obs_lat, "lon": obs_lon},
        "heading": obs_heading,
        "is_moving": obs_is_moving,
        "bbox_2d": bbox_to_store,
    }

    # EMA position update: current position dominates; observation nudges it.
    alpha = POSITION_EMA_ALPHA * obs_confidence
    cur_lat = fused_obj["position"]["lat"]
    cur_lon = fused_obj["position"]["lon"]
    # if there is an entity which contains the RSU word amont the observers, and I am an RSU, use my reported lat, lon
    if any("rsu" in key.lower() for key in observers.keys()) and "rsu" in source_vehicle_id.lower():
        fused_obj["position"]["lat"] = obs_lat
        fused_obj["position"]["lon"] = obs_lon
    else:
        fused_obj["position"]["lat"] = cur_lat + alpha * (obs_lat - cur_lat)
        fused_obj["position"]["lon"] = cur_lon + alpha * (obs_lon - cur_lon)

    # EMA heading update via unit-vector interpolation to handle wrap-around.
    if obs_heading >= 0:
        cur_heading = fused_obj.get("heading", -1)
        if cur_heading < 0:
            # No previous heading – accept the observation directly.
            fused_obj["heading"] = obs_heading
        else:
            hx = math.cos(math.radians(cur_heading)) + alpha * (
                math.cos(math.radians(obs_heading)) - math.cos(math.radians(cur_heading))
            )
            hy = math.sin(math.radians(cur_heading)) + alpha * (
                math.sin(math.radians(obs_heading)) - math.sin(math.radians(cur_heading))
            )
            new_heading = math.degrees(math.atan2(hy, hx))
            if new_heading < 0:
                new_heading += 360
            fused_obj["heading"] = new_heading

    # Confidence: take the max across all active observers.
    fused_obj["confidence"] = max(
        o.get("confidence", 0.5) for o in observers.values()
    )

    # is_moving: true if any observer reports movement.
    fused_obj["is_moving"] = any(o.get("is_moving", False) for o in observers.values())

    fused_obj["last_update"] = timestamp
    fused_obj["observers"] = observers
    fused_obj["observation_count"] = len(observers)

    return fused_obj


def create_fused_object(
    obs: Dict[str, Any],
    source_vehicle_id: str,
    timestamp: str
) -> Dict[str, Any]:
    """Create a new passive fused object from a first observation."""
    fused_id = f"fused_{uuid.uuid4().hex[:12]}"
    obs_confidence = obs.get("confidence", 0.5)
    obs_lat = obs["position_3d"]["lat"]
    obs_lon = obs["position_3d"]["lon"]
    obs_heading = obs.get("position_3d", {}).get("movement_heading", -1)
    obs_is_moving = obs.get("position_3d", {}).get("is_moving", False)
    obs_bbox = obs.get("bbox_2d") if isinstance(obs.get("bbox_2d"), list) and len(obs.get("bbox_2d")) == 4 else []

    fused_obj = {
        "fused_id": fused_id,
        "class": obs.get("class", "unknown"),
        "position": {"lat": obs_lat, "lon": obs_lon},
        "heading": obs_heading,
        "is_moving": obs_is_moving,
        "confidence": obs_confidence,
        "last_update": timestamp,
        "type": "passive",
        "observers": {
            source_vehicle_id: {
                "track_id": obs.get("track_id"),
                "last_seen": timestamp,
                "confidence": obs_confidence,
                "position": {"lat": obs_lat, "lon": obs_lon},
                "heading": obs_heading,
                "is_moving": obs_is_moving,
                "bbox_2d": obs_bbox,
            }
        },
        "observation_count": 1,
    }
    return fused_obj


def expire_stale_objects() -> int:
    """Remove fused objects and egos that haven't been updated within their TTL."""
    now = time.time()
    removed = 0

    all_fused_members = redis_client.zrange(FUSED_GEO_KEY, 0, -1)
    for fused_id in all_fused_members:
        obj = get_fused_object(fused_id)
        if obj is None:
            redis_client.zrem(FUSED_GEO_KEY, fused_id)
            removed += 1
            continue
        try:
            last = datetime.fromisoformat(obj["last_update"]).timestamp()
        except (KeyError, ValueError):
            last = 0
        if now - last > FUSED_OBJECT_TTL_S:
            remove_fused_object(fused_id)
            removed += 1

    all_ego_members = redis_client.zrange(EGO_GEO_KEY, 0, -1)
    for vehicle_id in all_ego_members:
        ego = get_ego(vehicle_id)
        if ego is None:
            redis_client.zrem(EGO_GEO_KEY, vehicle_id)
            continue
        try:
            last = datetime.fromisoformat(ego["last_update"]).timestamp()
        except (KeyError, ValueError):
            last = 0
        if now - last > EGO_TTL_S:
            redis_client.delete(f"{EGO_PREFIX}{vehicle_id}")
            redis_client.zrem(EGO_GEO_KEY, vehicle_id)

    return removed


def fuse_observations(
    positions: List[Dict[str, Any]],
    source_vehicle_id: str,
    timestamp: str
) -> List[Dict[str, Any]]:
    """
    Core fusion: for each observed position, associate with an existing ego,
    existing fused object, or create a new passive participant.
    Returns the list of fused objects that were touched.
    """
    touched: Dict[str, Dict[str, Any]] = {}

    for obs in positions:
        pos_3d = obs.get("position_3d", {})
        obs_lat = pos_3d.get("lat")
        obs_lon = pos_3d.get("lon")
        if obs_lat is None or obs_lon is None:
            continue

        obs_class = obs.get("class", "unknown")
        obs_heading = pos_3d.get("movement_heading", -1)
        obs_is_moving = pos_3d.get("is_moving", False)

        best_cost = float('inf')
        best_match_id: Optional[str] = None
        best_match_type: Optional[str] = None  # "ego" or "fused"

        # 1. Search ego vehicles
        ego_candidates = geo_search_candidates(EGO_GEO_KEY, obs_lon, obs_lat, GATING_RADIUS_M if obs_class == "car" else GATING_RADIUS_M_PERSON)
        print("Ego candidates: ", ego_candidates)
        for ego_id, _geo_dist in ego_candidates:
            if ego_id == source_vehicle_id:
                continue
            ego = get_ego(ego_id)
            if ego is None:
                continue

            cost = association_cost(
                obs_lat, obs_lon, obs_class, obs_heading, obs_is_moving,
                ego["position"]["lat"], ego["position"]["lon"],
                "car", ego.get("heading", -1), True
            )
            print("Ego: ", ego_id, ego["position"]["lat"], ego["position"]["lon"], ego.get("heading", -1), ego.get("is_moving", False), "car")
            print("Observation: ", obs_lat, obs_lon, obs_heading, obs_is_moving, obs_class)
            print("Cost for ego: ", ego_id, cost)
            if cost < best_cost:
                best_cost = cost
                best_match_id = ego_id
                best_match_type = "ego"
                print("Found ego for fusion")

        # 2. Search existing fused objects
        fused_candidates = geo_search_candidates(FUSED_GEO_KEY, obs_lon, obs_lat, GATING_RADIUS_M if obs_class == "car" else GATING_RADIUS_M_PERSON)
        for fused_id, _geo_dist in fused_candidates:
            fobj = get_fused_object(fused_id)
            if fobj is None:
                continue
            cost = association_cost(
                obs_lat, obs_lon, obs_class, obs_heading, obs_is_moving,
                fobj["position"]["lat"], fobj["position"]["lon"],
                fobj.get("class", "unknown"), fobj.get("heading", -1),
                fobj.get("is_moving", False)
            )
            if cost < best_cost:
                best_cost = cost
                best_match_id = fused_id
                best_match_type = "fused"
                print("Found fused for fusion")

        # 3. Decision
        if best_cost < ASSOCIATION_THRESHOLD and best_match_type == "ego":
            logger.debug(
                f"Observation matched ego {best_match_id} (cost={best_cost:.2f}), skipping"
            )
            continue

        if best_cost < ASSOCIATION_THRESHOLD and best_match_type == "fused":
            fobj = get_fused_object(best_match_id)
            if fobj is not None:
                fobj = merge_observation_into_fused(fobj, obs, source_vehicle_id, timestamp)
                save_fused_object(fobj)
                touched[fobj["fused_id"]] = fobj
                logger.debug(
                    f"Merged observation into fused object {best_match_id} (cost={best_cost:.2f})"
                )
                continue

        # No match -- create new passive participant
        new_obj = create_fused_object(obs, source_vehicle_id, timestamp)
        save_fused_object(new_obj)
        touched[new_obj["fused_id"]] = new_obj
        logger.debug(f"Created new fused object {new_obj['fused_id']}")

    return list(touched.values())


def build_fusion_result(
    vehicle_id: str,
    timestamp: str,
    fused_objects: List[Dict[str, Any]],
    metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """Build the output message to publish on the fusion-results-stream."""
    positions = []
    for fobj in fused_objects:
        positions.append({
            "track_id": fobj["fused_id"], # for backward compatibility
            "fused_id": fobj["fused_id"],
            "class": fobj.get("class", "unknown"),
            "position_3d": {
                "lat": fobj["position"]["lat"],
                "lon": fobj["position"]["lon"],
                "movement_heading": fobj.get("heading", -1),
                "is_moving": fobj.get("is_moving", False),
            },
            "confidence": fobj.get("confidence", 0),
            "type": fobj.get("type", "passive"),
            "observation_count": fobj.get("observation_count", 1),
            "observers": fobj.get("observers", {}),
        })
    print(positions)
    # del metadata["rtp_metadata"] # only for testing, remove rtp_metadata from the output
    return {
        "vehicle_id": vehicle_id,
        "timestamp": timestamp,
        "positions": positions,
        "metadata": metadata,
        "fused_object_count": len(positions),
    }


async def process_message_and_fuse(message: aio_pika.IncomingMessage):
    """Consume a position-results message, run fusion, and publish the result."""
    async with message.process():
        message_id = None
        data = None
        try:
            data = json.loads(message.body.decode())

            if MONITORING_ENABLED and tsdb:
                message_id = extract_message_id(data)

            vehicle_id = data.get("vehicle_id", "unknown")
            timestamp = data.get("timestamp", datetime.now().isoformat())
            positions = data.get("positions", [])
            metadata = data.get("metadata", {})

            logger.info(
                f"Fusing {len(positions)} observations from ego {vehicle_id}"
            )

            start_time = time.time()

            # 1. Register / update ego vehicle
            upsert_ego_vehicle(vehicle_id, metadata, timestamp)

            # 2. Run fusion
            if "rsu" in vehicle_id.lower():
              fused_objects = fuse_observations(positions, vehicle_id, timestamp)
            else:
              fused_objects = []

            # 3. Expire stale objects periodically (cheap enough to run inline)
            removed = expire_stale_objects()
            if removed:
                logger.info(f"Expired {removed} stale fused objects")

            # 4. Build and publish result
            result = build_fusion_result(vehicle_id, timestamp, fused_objects, metadata)
            if message_id:
                result["message_id"] = message_id

            processing_ms = int((time.time() - start_time) * 1000)

            if MONITORING_ENABLED and tsdb and message_id:
                tsdb.record_message_processing(
                    message_id=message_id,
                    service_name="ru-fusion",
                    processing_time_ms=processing_ms,
                    status="success",
                    vehicle_id=vehicle_id,
                    queue_name=POSITION_QUEUE,
                    metadata={
                        "fused_objects_count": len(fused_objects),
                        "expired_count": removed,
                    }
                )

            print(result)
            await publish_fusion_result(result)

        except Exception as e:
            if MONITORING_ENABLED and tsdb and message_id:
                tsdb.record_message_processing(
                    message_id=message_id,
                    service_name="ru-fusion",
                    processing_time_ms=0,
                    status="error",
                    vehicle_id=data.get("vehicle_id") if data else None,
                    queue_name=POSITION_QUEUE,
                    metadata={"error": str(e)}
                )
            logger.error(f"Error in fusion processing: {str(e)}", exc_info=True)


async def start_consumer():
    """Start the RabbitMQ consumer service."""
    try:
        logger.info("Starting Road User Fusion Service")
        
        # Create RabbitMQ connection
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        
        async with connection:
            # Create channel
            channel = await connection.channel()
            
            # Declare queues
            position_queue = await channel.get_queue(
                POSITION_QUEUE,
            )
            
            # Start consuming from both queues
            await position_queue.consume(process_message_and_fuse)
            
            logger.info(f"Started consuming from queues: {POSITION_QUEUE}")
            
            # Keep the consumer running
            await asyncio.Future()
            
    except Exception as e:
        logger.error(f"Error in consumer: {str(e)}")
        # Wait before retrying
        await asyncio.sleep(5)
        await start_consumer()

if __name__ == "__main__":
    logger.info("Starting Road User Fusion Service")
    asyncio.run(start_consumer())
