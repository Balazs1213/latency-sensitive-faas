from typing import Any, Optional, NamedTuple, Dict
from parliament import Context  # type: ignore

class Event(NamedTuple):
    """
    Represents an event to pass to components.
    """
    json: Optional[Dict[str, Any]]
    data: Optional[bytes]
      
def extract_event(context: Context) -> Event:
    """
    Extracts the event from the context.
    """
    json_data = None
    byte_data = None
    
    if context.request.is_json:
        json_data = context.request.json
    else:
        byte_data = context.request.data

    return Event(json=json_data, data=byte_data)

def create_event(data: bytes | Dict[str, Any]) -> Event:
    """
    Creates an event from the context.
    """
    return Event(
        json=data if isinstance(data, dict) else None,
        data=data if isinstance(data, bytes) else None,
    )
                    