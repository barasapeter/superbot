from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
import uuid
from datetime import datetime
import asyncio
import json
from typing import Optional
import logging

from persistent_worker import PersistentWorker, workers
from redis_manager import redis_manager
from ws_log_streaming import ws_manager, redis_log_listener
from models import WorkerConfig, WorkerResponse

# Add monitor router import
from routers import monitor, websocket_monitor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== STARTUP/SHUTDOWN ====================


async def restore_workers():
    """Restore workers from Redis state"""
    all_workers = await redis_manager.get_all_workers()

    for worker_state in all_workers:
        worker_id = worker_state.get("worker_id")
        status = worker_state.get("status")

        # Only restore running workers
        if status == "running":
            config = worker_state.get("config", {})

            # Handle config if it's a string (JSON)
            if isinstance(config, str):
                try:
                    config = json.loads(config)
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse config for worker {worker_id}")
                    continue

            if config and isinstance(config, dict):
                try:
                    worker = PersistentWorker(worker_id, config)
                    workers[worker_id] = worker
                    await worker.start()
                    logger.info(f"🔄 Restored worker {worker_id}")
                except Exception as e:
                    logger.error(f"Failed to restore worker {worker_id}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- Startup ----
    logger.info("🚀 Starting server...")

    # Connect to Redis
    await redis_manager.connect()
    logger.info("✅ Redis connected")

    # Start Redis listener in background (if needed)
    # listener_task = asyncio.create_task(redis_log_listener())

    # Restore workers from previous sessions
    await restore_workers()

    logger.info(f"🚀 Server started with {len(workers)} workers restored")

    try:
        yield
    finally:
        # ---- Shutdown ----
        logger.info("🛑 Shutting down server...")

        # Stop all workers
        for worker_id in list(workers.keys()):
            try:
                await workers[worker_id].stop()
                logger.info(f"Stopped worker {worker_id}")
            except Exception as e:
                logger.error(f"Error stopping worker {worker_id}: {e}")

        # Cancel the Redis listener task
        # listener_task.cancel()
        # try:
        #     await listener_task
        # except asyncio.CancelledError:
        #     pass

        # Disconnect Redis
        await redis_manager.disconnect()
        logger.info("🛑 Server shutdown complete")


app = FastAPI(title="Persistent Trading Bot API", version="1.0", lifespan=lifespan)

# Include monitor routers
app.include_router(monitor.router)
app.include_router(websocket_monitor.router)

# ==================== WORKER MANAGEMENT ====================


@app.post("/workers/start", response_model=WorkerResponse)
async def start_worker(config: WorkerConfig):
    """Start a new persistent worker"""
    worker_id = str(uuid.uuid4())[:8]

    # Check if worker already exists
    if worker_id in workers:
        raise HTTPException(400, "Worker already exists")

    # Create and start worker
    config_dict = config.model_dump()
    worker = PersistentWorker(worker_id, config_dict)
    workers[worker_id] = worker

    await worker.start()

    return WorkerResponse(
        worker_id=worker_id,
        status="running",
        message="Worker started successfully",
        started_at=datetime.now().isoformat(),
    )


@app.post("/workers/{worker_id}/stop")
async def stop_worker(worker_id: str):
    """Stop a running worker"""
    if worker_id not in workers:
        raise HTTPException(404, f"Worker {worker_id} not found")

    await workers[worker_id].stop()

    return {
        "worker_id": worker_id,
        "status": "stopped",
        "message": "Worker stopped successfully",
    }


@app.post("/workers/{worker_id}/restart")
async def restart_worker(worker_id: str):
    """Restart a worker"""
    if worker_id not in workers:
        raise HTTPException(404, f"Worker {worker_id} not found")

    # Stop existing
    await workers[worker_id].stop()

    # Get config and start again
    worker_state = await redis_manager.get_worker_state(worker_id)

    # Handle config parsing
    config = worker_state.get("config", {})
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            raise HTTPException(400, "Invalid worker config")

    if not worker_state or not config:
        raise HTTPException(400, "Worker config not found")

    # Create new worker with same config
    worker = PersistentWorker(worker_id, config)
    workers[worker_id] = worker
    await worker.start()

    return {
        "worker_id": worker_id,
        "status": "running",
        "message": "Worker restarted successfully",
    }


@app.get("/workers", response_model=list)
async def list_workers():
    """List all workers with their status"""
    result = []
    for worker_id, worker in workers.items():
        state = await redis_manager.get_worker_state(worker_id)
        if state and "config" in state:
            try:
                state["config"] = (
                    json.loads(state["config"])
                    if isinstance(state["config"], str)
                    else state["config"]
                )
            except:
                pass

        result.append(
            {
                "worker_id": worker_id,
                "status": "running" if worker.is_running else "stopped",
                "is_running": worker.is_running,
                "state": state,
            }
        )
    return result


@app.get("/workers/{worker_id}/status")
async def get_worker_status(worker_id: str):
    """Get detailed status of a specific worker"""
    if worker_id not in workers:
        raise HTTPException(404, f"Worker {worker_id} not found")

    state = await redis_manager.get_worker_state(worker_id)
    if state and "config" in state:
        try:
            state["config"] = (
                json.loads(state["config"])
                if isinstance(state["config"], str)
                else state["config"]
            )
        except:
            pass

    return {
        "worker_id": worker_id,
        "is_running": workers[worker_id].is_running,
        "state": state,
    }


@app.get("/workers/{worker_id}/logs")
async def get_worker_logs(worker_id: str, limit: int = 100):
    """Get log history for a worker"""
    if worker_id not in workers:
        raise HTTPException(404, f"Worker {worker_id} not found")

    logs = await workers[worker_id].get_logs(limit)
    return {"worker_id": worker_id, "logs": logs, "count": len(logs)}


@app.get("/workers/{worker_id}/events")
async def get_worker_events(
    worker_id: str, event_type: Optional[str] = None, limit: int = 100
):
    """Get events for a worker"""
    if worker_id not in workers:
        raise HTTPException(404, f"Worker {worker_id} not found")

    events = await workers[worker_id].get_events(event_type, limit)
    return {
        "worker_id": worker_id,
        "events": events,
        "count": len(events),
        "event_types": (
            await redis_manager.get_all_event_types(worker_id)
            if not event_type
            else [event_type]
        ),
    }


@app.get("/workers/{worker_id}/events/latest")
async def get_latest_worker_event(worker_id: str, event_type: str):
    """Get the latest event of a specific type"""
    if worker_id not in workers:
        raise HTTPException(404, f"Worker {worker_id} not found")

    event = await redis_manager.get_latest_event(worker_id, event_type)
    if not event:
        raise HTTPException(
            404, f"No event of type {event_type} found for worker {worker_id}"
        )

    return event


@app.get("/workers/{worker_id}/events/types")
async def get_worker_event_types(worker_id: str):
    """Get all event types for a worker"""
    if worker_id not in workers:
        raise HTTPException(404, f"Worker {worker_id} not found")

    types = await redis_manager.get_all_event_types(worker_id)
    return {"worker_id": worker_id, "event_types": types}


# ==================== WEBSOCKET ENDPOINTS ====================


@app.websocket("/ws/logs/{worker_id}")
async def websocket_logs(websocket: WebSocket, worker_id: str):
    """
    WebSocket endpoint for real-time log streaming.
    Includes history replay on connection.
    """
    connection_id = str(uuid.uuid4())[:8]

    # Check if worker exists
    if worker_id not in workers:
        await websocket.accept()
        await websocket.send_json(
            {"type": "error", "message": f"Worker {worker_id} not found"}
        )
        await websocket.close()
        return

    try:
        # Accept connection
        await ws_manager.connect(websocket, worker_id, connection_id)

        # Send initial connection confirmation
        await websocket.send_json(
            {
                "type": "connected",
                "worker_id": worker_id,
                "message": "Connected to log stream",
                "timestamp": datetime.now().isoformat(),
            }
        )

        # Send recent history (last 100 logs)
        history = await workers[worker_id].get_logs(limit=100)
        if history:
            await websocket.send_json(
                {
                    "type": "history",
                    "worker_id": worker_id,
                    "logs": history,
                    "count": len(history),
                }
            )

        # Send current status
        state = await redis_manager.get_worker_state(worker_id)
        if state and "config" in state:
            try:
                state["config"] = (
                    json.loads(state["config"])
                    if isinstance(state["config"], str)
                    else state["config"]
                )
            except:
                pass

        await websocket.send_json(
            {"type": "status", "worker_id": worker_id, "data": state}
        )

        # Keep connection alive
        while True:
            # Wait for client messages (ping/pong)
            try:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
                elif data == "get_status":
                    state = await redis_manager.get_worker_state(worker_id)
                    if state and "config" in state:
                        try:
                            state["config"] = (
                                json.loads(state["config"])
                                if isinstance(state["config"], str)
                                else state["config"]
                            )
                        except:
                            pass
                    await websocket.send_json(
                        {"type": "status", "worker_id": worker_id, "data": state}
                    )
            except WebSocketDisconnect:
                break

    except WebSocketDisconnect:
        pass
    finally:
        # Clean up connection
        await ws_manager.disconnect(websocket, worker_id, connection_id)


@app.websocket("/ws/logs")
async def websocket_all_logs(websocket: WebSocket):
    """
    WebSocket endpoint for streaming logs from all workers.
    """
    await websocket.accept()

    try:
        # Send list of active workers
        active_workers = list(workers.keys())
        await websocket.send_json({"type": "workers", "workers": active_workers})

        # Subscribe to all worker logs
        # This will get logs from Redis pub/sub for all workers
        while True:
            try:
                # Listen for logs from Redis
                if redis_manager.pubsub:
                    message = await redis_manager.pubsub.get_message(
                        timeout=1.0, ignore_subscribe_messages=True
                    )

                    if message and message["type"] == "message":
                        # Check if it's a log message
                        channel = message["channel"]
                        if channel.startswith("worker:") and channel.endswith(":logs"):
                            try:
                                log_entry = json.loads(message["data"])
                                await websocket.send_json(
                                    {"type": "log", "data": log_entry}
                                )
                            except Exception:
                                pass

                # Check for client messages
                try:
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=0.1)
                    if data == "ping":
                        await websocket.send_text("pong")
                    elif data == "list_workers":
                        await websocket.send_json(
                            {"type": "workers", "workers": list(workers.keys())}
                        )
                except asyncio.TimeoutError:
                    pass
                except WebSocketDisconnect:
                    break

            except asyncio.CancelledError:
                break

    except WebSocketDisconnect:
        pass
    finally:
        await websocket.close()


# ==================== HEALTH CHECK ====================


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "active_workers": len(workers),
        "running_workers": sum(1 for w in workers.values() if w.is_running),
        "timestamp": datetime.now().isoformat(),
    }


# ==================== RUN SERVER ====================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
