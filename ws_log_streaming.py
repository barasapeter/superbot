# websocket_logger.py
from fastapi import WebSocket
import asyncio
import json
from typing import Dict, Set
from redis_manager import redis_manager
from datetime import datetime


class WebSocketConnectionManager:
    """Manages WebSocket connections for log streaming"""

    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.subscriptions: Dict[str, Set[str]] = (
            {}
        )  # worker_id -> set of connection IDs

    async def connect(self, websocket: WebSocket, worker_id: str, connection_id: str):
        """Connect a WebSocket client"""
        await websocket.accept()

        if worker_id not in self.active_connections:
            self.active_connections[worker_id] = set()
        self.active_connections[worker_id].add(websocket)

        if connection_id not in self.subscriptions:
            self.subscriptions[connection_id] = set()
        self.subscriptions[connection_id].add(worker_id)

    async def disconnect(
        self, websocket: WebSocket, worker_id: str, connection_id: str
    ):
        """Disconnect a WebSocket client"""
        if worker_id in self.active_connections:
            self.active_connections[worker_id].discard(websocket)
            if not self.active_connections[worker_id]:
                del self.active_connections[worker_id]

        if connection_id in self.subscriptions:
            self.subscriptions[connection_id].discard(worker_id)
            if not self.subscriptions[connection_id]:
                del self.subscriptions[connection_id]

    async def broadcast_log(self, worker_id: str, log_entry: Dict):
        """Broadcast a log to all connected clients for a worker"""
        if worker_id not in self.active_connections:
            return

        # Send log as JSON
        log_json = json.dumps(
            {
                "type": "log",
                "worker_id": worker_id,
                "data": log_entry,
                "timestamp": datetime.now().isoformat(),
            }
        )

        # Broadcast to all connections for this worker
        for websocket in self.active_connections.get(worker_id, set()):
            try:
                await websocket.send_text(log_json)
            except Exception:
                # Failed to send, remove connection
                pass


# Global WebSocket manager
ws_manager = WebSocketConnectionManager()


# Background task to listen to Redis pub/sub
async def redis_log_listener():
    """Listen to Redis pub/sub and forward to WebSocket clients"""
    while True:
        try:
            message = await redis_manager.pubsub.get_message(
                timeout=1.0, ignore_subscribe_messages=True
            )

            if message and message["type"] == "message":
                channel = message["channel"]
                # Extract worker_id from channel
                if channel.startswith("worker:"):
                    parts = channel.split(":")
                    if len(parts) >= 2:
                        worker_id = parts[1]
                        try:
                            log_entry = json.loads(message["data"])
                            await ws_manager.broadcast_log(worker_id, log_entry)
                        except Exception:
                            pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Redis listener error: {e}")
            await asyncio.sleep(1)
