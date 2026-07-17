# persistent_worker.py
import asyncio
import logging
import sys
from datetime import datetime
from typing import Dict, Any, Optional

# Import your existing worker function and components
from worker import run_worker, ColorFormatter
from redis_manager import redis_manager


class PersistentWorker:
    """Wrapper for running workers with persistence"""

    def __init__(self, worker_id: str, config: Dict[str, Any]):
        self.worker_id = worker_id
        self.config = config
        self.task: Optional[asyncio.Task] = None
        self.is_running = False
        self.is_stopping = False
        self.logger = None
        self.stats = None

        # Create dedicated logger for this worker
        self._setup_logger()

    def _setup_logger(self):
        """Set up a logger that also stores logs in Redis"""

        class PersistentHandler(logging.Handler):
            def __init__(self, worker_id, redis_mgr):
                super().__init__()
                self.worker_id = worker_id
                self.redis_mgr = redis_mgr
                self.log_buffer = []

            def emit(self, record):
                log_entry = {
                    "level": record.levelname,
                    "message": self.format(record),
                    "timestamp": datetime.now().isoformat(),
                }

                # Store in Redis asynchronously
                asyncio.create_task(self.redis_mgr.store_log(self.worker_id, log_entry))

        # Setup logger
        self.logger = logging.getLogger(f"worker_{self.worker_id}")
        self.logger.setLevel(logging.INFO)

        # Remove existing handlers
        self.logger.handlers.clear()

        # Add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ColorFormatter())
        self.logger.addHandler(console_handler)

        # Add persistent handler
        persistent_handler = PersistentHandler(self.worker_id, redis_manager)
        persistent_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        self.logger.addHandler(persistent_handler)

    async def start(self):
        """Start the worker"""
        if self.is_running:
            self.logger.warning(f"Worker {self.worker_id} is already running")
            return

        self.is_running = True
        self.is_stopping = False

        # Save initial state
        await redis_manager.save_worker_state(
            self.worker_id,
            {
                "status": "starting",
                "started_at": datetime.now().isoformat(),
                "config": self.config,
            },
        )

        # Create task
        self.task = asyncio.create_task(self._run())

        # Store task in global registry
        worker_tasks[self.worker_id] = self.task

        self.logger.info(f"🚀 Worker {self.worker_id} started")

    async def _run(self):
        """Main worker execution with persistence"""
        try:
            # Save running state
            await redis_manager.save_worker_state(
                self.worker_id,
                {"status": "running", "last_heartbeat": datetime.now().isoformat()},
            )

            # Run the actual worker
            # We need to capture the stats from the worker
            # This requires modifying the run_worker function to return stats
            self.stats = await run_worker(self.config, self.logger)

            # Save completion state
            if self.stats:
                await redis_manager.save_worker_state(
                    self.worker_id,
                    {
                        "status": "completed",
                        "completed_at": datetime.now().isoformat(),
                        "trades": self.stats.trades,
                        "net_pl": self.stats.net_pl,
                    },
                )
            else:
                await redis_manager.save_worker_state(
                    self.worker_id, {"status": "completed"}
                )

        except asyncio.CancelledError:
            self.logger.info(f"🛑 Worker {self.worker_id} stopped by user")
            await redis_manager.save_worker_state(
                self.worker_id,
                {"status": "stopped", "stopped_at": datetime.now().isoformat()},
            )

        except Exception as e:
            self.logger.error(f"❌ Worker {self.worker_id} failed: {e}")
            await redis_manager.save_worker_state(
                self.worker_id,
                {
                    "status": "error",
                    "error": str(e),
                    "failed_at": datetime.now().isoformat(),
                },
            )

        finally:
            self.is_running = False
            # Clean up task reference
            if self.worker_id in worker_tasks:
                del worker_tasks[self.worker_id]

    async def stop(self):
        """Stop the worker gracefully"""
        if not self.is_running:
            self.logger.warning(f"Worker {self.worker_id} is not running")
            return

        self.is_stopping = True
        self.logger.info(f"⏹️ Stopping worker {self.worker_id}...")

        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

        self.is_running = False
        self.logger.info(f"✅ Worker {self.worker_id} stopped")

    async def get_logs(self, limit: int = 100, since: Optional[datetime] = None):
        """Get logs for this worker"""
        return await redis_manager.get_log_history(self.worker_id, limit, since)


# Global worker registry
worker_tasks: Dict[str, asyncio.Task] = {}
workers: Dict[str, PersistentWorker] = {}
