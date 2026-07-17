# main.py - FastAPI Application
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import asyncio
import uuid
from datetime import datetime
import logging
from typing import List

# Import your worker function
from worker import run_worker, C

app = FastAPI(title="Deriv Trading Bot API", version="1.0")

# Store active workers
active_workers: Dict[str, Dict[str, Any]] = {}
worker_tasks: Dict[str, asyncio.Task] = {}


# ==================== PYDANTIC MODELS ====================
class WorkerConfig(BaseModel):
    api_token: str = Field(..., description="Deriv API token")
    symbol: str = Field("R_100", description="Trading symbol")
    currency: str = Field("USD", description="Currency")
    app_id: str = Field("1089", description="App ID")
    account_type: str = Field("demo", description="Account type (demo/real)")
    min_stake: float = Field(0.35, ge=0.01, description="Minimum stake")
    initial_stake_percentage: float = Field(
        0.5, ge=0.1, le=10.0, description="Initial stake as % of balance"
    )
    target_streak: int = Field(4, ge=1, le=20, description="Target tick streak")
    contract_duration: int = Field(
        5, ge=1, le=10, description="Contract duration in ticks"
    )
    cooldown_seconds: int = Field(6, ge=1, le=30, description="Cooldown between trades")
    max_latency_ms: int = Field(
        350, ge=50, le=2000, description="Max latency before skipping"
    )
    martingale_enabled: bool = Field(True, description="Enable martingale")
    martingale_multiplier: float = Field(
        2.0, ge=1.1, le=5.0, description="Martingale multiplier"
    )
    max_martingale_steps: int = Field(
        7, ge=1, le=15, description="Max martingale steps"
    )
    max_stake: float = Field(5000, ge=1, description="Maximum stake allowed")
    heartbeat_interval: int = Field(5, ge=1, le=30, description="Heartbeat interval")
    max_reconnect_attempts: int = Field(
        5, ge=1, le=20, description="Max reconnect attempts"
    )
    stop_loss_percent: float = Field(
        100, ge=10, le=200, description="Stop loss percentage"
    )
    poll_timeout: int = Field(25, ge=5, le=60, description="Poll timeout")
    poll_interval: float = Field(0.3, ge=0.1, le=1.0, description="Poll interval")
    signal_cooldown: float = Field(1.0, ge=0.5, le=5.0, description="Signal cooldown")


class WorkerResponse(BaseModel):
    worker_id: str
    status: str
    message: str
    started_at: str


class WorkerStatus(BaseModel):
    worker_id: str
    status: str
    started_at: str
    config: Dict[str, Any]


# ==================== BACKGROUND WORKER MANAGER ====================
class WorkerManager:
    def __init__(self):
        self.workers = {}
        self.tasks = {}
        self._shutdown = False

    async def start_worker(self, config: Dict[str, Any]) -> str:
        """Start a new worker with given config"""
        worker_id = str(uuid.uuid4())[:8]

        # Create task
        task = asyncio.create_task(
            self._run_worker_task(worker_id, config), name=f"worker_{worker_id}"
        )

        self.workers[worker_id] = {
            "config": config,
            "status": "starting",
            "started_at": datetime.now().isoformat(),
            "task": task,
        }
        self.tasks[worker_id] = task

        return worker_id

    async def _run_worker_task(self, worker_id: str, config: Dict[str, Any]):
        """Wrapper for running worker with status tracking"""
        try:
            self.workers[worker_id]["status"] = "running"

            # Run the worker
            stats = await run_worker(config)

            # Worker completed normally
            self.workers[worker_id]["status"] = "completed"
            self.workers[worker_id]["stats"] = stats

        except asyncio.CancelledError:
            self.workers[worker_id]["status"] = "stopped"
            logging.info(f"Worker {worker_id} was stopped")

        except Exception as e:
            self.workers[worker_id]["status"] = "error"
            self.workers[worker_id]["error"] = str(e)
            logging.error(f"Worker {worker_id} failed: {e}")

        finally:
            # Clean up
            if worker_id in self.tasks:
                del self.tasks[worker_id]

    async def stop_worker(self, worker_id: str) -> bool:
        """Stop a running worker"""
        if worker_id not in self.tasks:
            return False

        task = self.tasks[worker_id]
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        self.workers[worker_id]["status"] = "stopped"
        return True

    def get_worker_status(self, worker_id: str) -> Optional[Dict]:
        """Get status of a specific worker"""
        return self.workers.get(worker_id)

    def list_workers(self) -> List[Dict]:
        """List all workers"""
        return [
            {
                "worker_id": wid,
                "status": info["status"],
                "started_at": info["started_at"],
                "config": info.get("config", {}),
            }
            for wid, info in self.workers.items()
        ]

    async def shutdown_all(self):
        """Shutdown all workers"""
        for worker_id in list(self.tasks.keys()):
            await self.stop_worker(worker_id)


# Initialize worker manager
worker_manager = WorkerManager()

# ==================== FASTAPI ENDPOINTS ====================


@app.on_event("startup")
async def startup_event():
    """Startup event handler"""
    logging.info("🚀 FastAPI server starting...")
    logging.info(f"{C.GREEN}✅ Worker manager initialized{C.RESET}")


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event handler"""
    logging.info("🛑 Shutting down server...")
    await worker_manager.shutdown_all()
    logging.info("✅ All workers stopped")


@app.post("/workers/start", response_model=WorkerResponse)
async def start_worker(config: WorkerConfig, background_tasks: BackgroundTasks):
    """
    Start a new trading bot worker with the given configuration.
    This will run in the background.
    """
    try:
        # Convert to dict
        config_dict = config.dict()

        # Start worker
        worker_id = await worker_manager.start_worker(config_dict)

        return WorkerResponse(
            worker_id=worker_id,
            status="starting",
            message="Worker started successfully",
            started_at=datetime.now().isoformat(),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start worker: {str(e)}")


@app.post("/workers/{worker_id}/stop")
async def stop_worker(worker_id: str):
    """Stop a running worker"""
    success = await worker_manager.stop_worker(worker_id)

    if not success:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    return {
        "worker_id": worker_id,
        "status": "stopped",
        "message": "Worker stopped successfully",
    }


@app.get("/workers", response_model=List[WorkerStatus])
async def list_workers():
    """List all workers and their status"""
    workers = worker_manager.list_workers()
    return workers


@app.get("/workers/{worker_id}/status")
async def get_worker_status(worker_id: str):
    """Get detailed status of a specific worker"""
    status = worker_manager.get_worker_status(worker_id)

    if not status:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    return {"worker_id": worker_id, **status}


@app.get("/workers/{worker_id}/logs")
async def get_worker_logs(worker_id: str, lines: int = 50):
    """Get recent logs from a worker"""
    # Note: You'll need to implement log capture/storage
    # This is a placeholder - you could use a log buffer or file
    if worker_id not in worker_manager.workers:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    return {"worker_id": worker_id, "logs": []}  # Implement log retrieval


# ==================== HEALTH CHECK ====================
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "active_workers": len(worker_manager.tasks),
        "timestamp": datetime.now().isoformat(),
    }


# ==================== RUN SERVER ====================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
