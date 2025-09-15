import os
import json
import asyncio
from typing import Any
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message
from realtime_state import connections

router = APIRouter(tags=["Alerts"])

PROJECT_ID = os.getenv("GCP_PROJECT_ID", os.getenv("GCP_PROJECT", "cement-operations-optimization"))
ALERTS_SUBSCRIPTION = os.getenv("ALERTS_SUBSCRIPTION", "cement-alerts-sub")  # pubsub subscription name

# Create an async subscriber client
subscriber_client = pubsub_v1.SubscriberClient()

subscription_path = subscriber_client.subscription_path(PROJECT_ID, ALERTS_SUBSCRIPTION)

# background task handle
_bg_task = None

async def broadcast(message: dict):
    """Send alert to all connected websockets"""
    dead = []
    text = json.dumps(message)
    for ws in list(connections):
        try:
            await ws.send_text(text)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            connections.remove(ws)
        except KeyError:
            pass

def _pubsub_callback(message: Message) -> None:
    """Synchronous callback called in a separate thread by subscriber. Use create_task to schedule."""
    try:
        payload = {}
        if message.data:
            try:
                payload = json.loads(message.data.decode('utf-8'))
            except Exception:
                payload = {"raw": message.data.decode('utf-8')}
        # schedule broadcast in the main loop
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(asyncio.create_task, broadcast(payload))
        message.ack()
    except Exception as e:
        print("Error in pubsub callback:", e)
        # nack will cause redelivery
        message.nack()

async def _start_pubsub_listener():
    """Starts the streaming pull listener in threadpool mode (non-blocking)."""
    # streaming_pull_future runs in background borrowed thread(s)
    streaming_pull_future = subscriber_client.subscribe(subscription_path, callback=_pubsub_callback)
    print(f"Started Pub/Sub listener on {subscription_path}")
    # keep the coroutine alive until cancelled
    try:
        await asyncio.wrap_future(streaming_pull_future)  # convert to awaitable
    except asyncio.CancelledError:
        streaming_pull_future.cancel()
    except Exception as e:
        print("Pub/Sub listener error:", e)

@router.on_event("startup")
async def startup_event():
    global _bg_task
    if _bg_task is None:
        # start background task to attach subscriber; run as background task
        _bg_task = asyncio.create_task(_start_pubsub_listener())

@router.on_event("shutdown")
async def shutdown_event():
    global _bg_task
    if _bg_task:
        _bg_task.cancel()
        _bg_task = None

@router.websocket("/ws/alerts")
async def websocket_alerts(ws: WebSocket):
    await ws.accept()
    connections.add(ws)
    try:
        # send a welcome / status message
        await ws.send_text(json.dumps({"type": "hello", "msg": "connected to alerts websocket"}))
        # keep connection alive until disconnect
        while True:
            # optional: wait for ping from client or sleep
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        pass
    finally:
        try:
            connections.remove(ws)
        except Exception:
            pass
