from typing import Set
from fastapi import WebSocket

#  this set is per Cloud Run instance.
connections: Set[WebSocket] = set()
