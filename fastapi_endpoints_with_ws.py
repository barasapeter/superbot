# main_api.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
import uuid
from datetime import datetime
import asyncio
import json

from persistent_worker import PersistentWorker, workers
from redis_manager import redis_manager
from ws_log_streaming import ws_manager, redis_log_listener
from models import WorkerConfig, WorkerResponse

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
            if config:
                worker = PersistentWorker(worker_id, config)
                workers[worker_id] = worker
                await worker.start()
                print(f"🔄 Restored worker {worker_id}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- Startup ----
    await redis_manager.connect()

    # Start Redis listener in background
    listener_task = asyncio.create_task(redis_log_listener())

    # Restore workers from previous sessions
    await restore_workers()

    print(f"🚀 Server started with {len(workers)} workers restored")

    try:
        yield
    finally:
        # ---- Shutdown ----
        # Stop all workers
        for worker_id in list(workers.keys()):
            await workers[worker_id].stop()

        # Cancel the Redis listener task
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass

        # Disconnect Redis
        await redis_manager.disconnect()
        print("🛑 Server shutdown complete")


app = FastAPI(title="Persistent Trading Bot API", version="1.0", lifespan=lifespan)


# ==================== WORKER MANAGEMENT ====================


@app.post("/workers/start", response_model=WorkerResponse)
async def start_worker(config: WorkerConfig):
    """Start a new persistent worker"""
    worker_id = str(uuid.uuid4())[:8]

    # Check if worker already exists
    if worker_id in workers:
        raise HTTPException(400, "Worker already exists")

    # Create and start worker
    worker = PersistentWorker(worker_id, config.model_dump())
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
    if not worker_state or "config" not in worker_state:
        raise HTTPException(400, "Worker config not found")

    # Create new worker with same config
    worker = PersistentWorker(worker_id, worker_state["config"])
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

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
