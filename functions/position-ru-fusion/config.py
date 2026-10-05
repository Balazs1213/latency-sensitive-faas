from position_service import handler as position_service
from ru_fusion_service import handler as ru_fusion_service
from redis import Redis
import os
import json
from typing import Dict, cast, Any, Callable
from parliament import Context  # type: ignore
from route import RoutingTable

REDIS_URL = os.environ["NODE_IP"]
APP_NAME = os.environ["APP_NAME"]
FUNCTION_NAME = os.environ["FUNCTION_NAME"]
redis_client = Redis(host=REDIS_URL, port=6379)

HANDLERS: Dict[str, Callable[[Context], Any]] = {
    "ru-fusion-service": ru_fusion_service,
    "position-service": position_service,
    # REGISTER COMPONENT HANDLERS HERE
}


def read_config() -> RoutingTable:
    """
    Reads configuration keys from Redis and populates HANDLERS dictionary.
    """
    config: Any = redis_client.get(FUNCTION_NAME)
    if not config:
        raise ValueError(f"Configuration for {FUNCTION_NAME} not found in Redis")

    configJson = json.loads(config)
    return cast(RoutingTable, configJson)
