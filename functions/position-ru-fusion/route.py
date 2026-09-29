from typing import Dict, List, TypedDict

class Route(TypedDict):
    """
    Represents a route in the routing table.
    """
    component: str
    url: str
    
RoutingTable = Dict[str, List[Route]]