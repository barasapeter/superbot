from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Set

from redis_manager import redis_manager
from persistent_worker import workers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/monitor/ws", tags=["websocket"])

# Store active WebSocket connections
active_connections: Dict[str, Set[WebSocket]] = {}


@router.websocket("/{worker_id}")
async def websocket_worker_monitor(websocket: WebSocket, worker_id: str):
    """WebSocket endpoint for real-time worker monitoring"""
    await websocket.accept()

    # Add to active connections
    if worker_id not in active_connections:
        active_connections[worker_id] = set()
    active_connections[worker_id].add(websocket)

    try:
        # Check if worker exists
        state = await redis_manager.get_worker_state(worker_id)
        if not state:
            await websocket.send_json(
                {"type": "error", "message": f"Worker {worker_id} not found"}
            )
            await websocket.close()
            return

        # Send initial data
        stats = await redis_manager.get_worker_stats(worker_id)
        events = await redis_manager.get_worker_event_stream(worker_id, limit=100)
        logs = await redis_manager.get_log_history(worker_id, limit=50)

        await websocket.send_json(
            {
                "type": "initial",
                "worker_id": worker_id,
                "is_running": stats.get("is_running", False),
                "state": state,
                "stats": {
                    "total_events": stats.get("total_events", 0),
                    "event_types": stats.get("event_types", {}),
                    "categories": stats.get("categories", {}),
                },
                "events": events,
                "logs": logs,
            }
        )

        # Subscribe to Redis channels
        pubsub = redis_manager.redis.pubsub()
        await pubsub.subscribe(f"worker:{worker_id}:events", f"worker:{worker_id}:logs")

        try:
            # Listen for new events
            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.1
                )

                if message:
                    try:
                        data = json.loads(message["data"])
                        channel = message["channel"]

                        if channel.endswith(":events"):
                            # New event
                            enhanced_event = {
                                "type": data.get("type", "unknown"),
                                "timestamp": data.get("timestamp"),
                                "data": data.get("data", {}),
                                "display": {
                                    "icon": redis_manager._get_event_icon(
                                        data.get("type", "unknown")
                                    ),
                                    "color": redis_manager._get_event_color(
                                        data.get("type", "unknown")
                                    ),
                                    "message": data.get("data", {}).get("message", ""),
                                },
                            }
                            await websocket.send_json(
                                {"type": "new_event", "event": enhanced_event}
                            )
                        elif channel.endswith(":logs"):
                            # New log
                            await websocket.send_json({"type": "new_log", "log": data})
                    except json.JSONDecodeError:
                        pass

                # Check if client sent any messages
                try:
                    client_msg = await asyncio.wait_for(
                        websocket.receive_text(), timeout=0.01
                    )
                    if client_msg == "ping":
                        await websocket.send_text("pong")
                    elif client_msg == "get_status":
                        # Send updated status
                        state = await redis_manager.get_worker_state(worker_id)
                        await websocket.send_json(
                            {
                                "type": "status_update",
                                "is_running": worker_id in workers
                                and workers[worker_id].is_running,
                                "state": state,
                            }
                        )
                except asyncio.TimeoutError:
                    pass
                except WebSocketDisconnect:
                    break

        except asyncio.CancelledError:
            raise
        finally:
            await pubsub.unsubscribe(
                f"worker:{worker_id}:events", f"worker:{worker_id}:logs"
            )
            await pubsub.close()

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for worker {worker_id}")
    except Exception as e:
        logger.error(f"WebSocket error for worker {worker_id}: {e}")
    finally:
        # Remove connection
        if worker_id in active_connections:
            active_connections[worker_id].discard(websocket)
            if not active_connections[worker_id]:
                del active_connections[worker_id]


@router.get("/connections/{worker_id}")
async def get_active_connections(worker_id: str):
    """Get number of active WebSocket connections for a worker"""
    count = len(active_connections.get(worker_id, set()))
    return {"worker_id": worker_id, "active_connections": count}
