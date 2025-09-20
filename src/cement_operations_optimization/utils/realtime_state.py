from typing import Set
from fastapi import WebSocket
import asyncio
import json

#  this set is per Cloud Run instance.
connections: Set[WebSocket] = set()

async def broadcast(message: dict):
    dead = []
    for ws in list(connections):
        try:
            await ws.send_text(json.dumps(message))
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            connections.remove(ws)
        except Exception:
            pass

# Example: periodically send a test message to all clients (for debugging)
async def test_sender():
    while True:
        await broadcast({"msg": "test message from backend"})
        await asyncio.sleep(5)
