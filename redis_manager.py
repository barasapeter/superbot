# redis_manager.py
import redis.asyncio as redis
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging


class RedisManager:
    """Manages Redis connections for worker state and logs"""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis = None
        self.pubsub = None
        self.logger = logging.getLogger(__name__)

    async def connect(self):
        """Connect to Redis"""
        self.redis = await redis.from_url(self.redis_url, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe("worker:logs")
        self.logger.info("✅ Redis connected")

    async def disconnect(self):
        """Disconnect from Redis"""
        if self.pubsub:
            await self.pubsub.unsubscribe("worker:logs")
            await self.pubsub.close()
        if self.redis:
            await self.redis.close()
        self.logger.info("✅ Redis disconnected")

    # ============ WORKER STATE ============
    async def save_worker_state(self, worker_id: str, state: Dict[str, Any]):
        """Save worker state to Redis"""
        key = f"worker:{worker_id}:state"
        state["updated_at"] = datetime.now().isoformat()
        await self.redis.hset(key, mapping=state)
        await self.redis.expire(key, 86400 * 7)  # 7 days TTL

    async def get_worker_state(self, worker_id: str) -> Optional[Dict]:
        """Get worker state from Redis"""
        key = f"worker:{worker_id}:state"
        data = await self.redis.hgetall(key)
        return data if data else None

    async def get_all_workers(self) -> List[Dict]:
        """Get all worker states"""
        keys = await self.redis.keys("worker:*:state")
        workers = []
        for key in keys:
            worker_id = key.split(":")[1]
            state = await self.get_worker_state(worker_id)
            if state:
                state["worker_id"] = worker_id
                workers.append(state)
        return workers

    async def delete_worker_state(self, worker_id: str):
        """Delete worker state"""
        await self.redis.delete(f"worker:{worker_id}:state")
        await self.redis.delete(f"worker:{worker_id}:logs:history")

    # ============ LOG STORAGE ============
    async def store_log(self, worker_id: str, log_entry: Dict[str, Any]):
        """Store a log entry with TTL"""
        # Store in history list (last 1000 entries)
        history_key = f"worker:{worker_id}:logs:history"
        log_entry["timestamp"] = datetime.now().isoformat()

        # Add to history
        await self.redis.lpush(history_key, json.dumps(log_entry))
        # Keep only last 1000 entries
        await self.redis.ltrim(history_key, 0, 999)
        # Set TTL on history
        await self.redis.expire(history_key, 86400 * 7)  # 7 days

        # Publish to real-time channel
        await self.redis.publish(f"worker:{worker_id}:logs", json.dumps(log_entry))

    async def get_log_history(
        self, worker_id: str, limit: int = 100, since: Optional[datetime] = None
    ) -> List[Dict]:
        """Get log history for a worker"""
        history_key = f"worker:{worker_id}:logs:history"

        # Get logs from Redis
        logs = await self.redis.lrange(history_key, 0, limit - 1)

        parsed_logs = []
        for log in logs:
            try:
                entry = json.loads(log)
                # Filter by timestamp if since provided
                if since:
                    log_time = datetime.fromisoformat(entry["timestamp"])
                    if log_time < since:
                        continue
                parsed_logs.append(entry)
            except:
                continue

        return parsed_logs

    # ============ PUB/SUB ============
    async def subscribe_to_logs(self, worker_id: str):
        """Subscribe to logs for a specific worker"""
        # This uses pattern matching
        await self.pubsub.subscribe(f"worker:{worker_id}:logs")
        return self.pubsub

    async def unsubscribe_from_logs(self, worker_id: str):
        """Unsubscribe from logs for a specific worker"""
        await self.pubsub.unsubscribe(f"worker:{worker_id}:logs")


# Global Redis instance
redis_manager = RedisManager()
