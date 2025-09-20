# pubsub_push.py (FastAPI router)
import base64
import json
from fastapi import APIRouter, Request, Header, HTTPException
from typing import Set
import asyncio

router = APIRouter()
# global set maintained by your websocket endpoint
connected_websockets = set()

@router.post("/pubsub/push")
async def pubsub_push(request: Request, x_goog_resource_state: str | None = Header(None)):
    """
    Pub/Sub push handler. Cloud Pub/Sub sends a JSON with "message" field base64 encoded.
    No auth validation shown — add verification (JWT signature) in production.
    """
    body = await request.json()
    msg = body.get("message")
    if not msg:
        raise HTTPException(status_code=400, detail="Bad Pub/Sub payload")

    data_b64 = msg.get("data")
    payload = {}
    if data_b64:
        try:
            payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
        except Exception:
            payload = {"raw": base64.b64decode(data_b64).decode("utf-8")}
    # Broadcast payload to connected websockets
    await broadcast_to_websockets(payload)
    # Return 200 to acknowledge Pub/Sub
    return {"status": "ok"}

async def broadcast_to_websockets(payload):
    print(f"Broadcasting to {len(connected_websockets)} websockets: {payload}")
    dead = []
    for ws in list(connected_websockets):
        try:
            await ws.send_text(json.dumps(payload))
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            connected_websockets.remove(ws)
        except KeyError:
            pass
