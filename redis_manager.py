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
        # Don't subscribe here - let individual components subscribe as needed
        self.logger.info("✅ Redis connected")

    async def disconnect(self):
        """Disconnect from Redis"""
        if self.pubsub:
            try:
                await self.pubsub.unsubscribe()
                await self.pubsub.close()
            except Exception as e:
                self.logger.warning(f"Error closing pubsub: {e}")
        if self.redis:
            await self.redis.close()
        self.logger.info("✅ Redis disconnected")

    async def ensure_pubsub(self):
        """Ensure pubsub is initialized and connected"""
        if self.pubsub is None or not self.pubsub.connection:
            # Recreate pubsub if needed
            if self.pubsub:
                try:
                    await self.pubsub.close()
                except:
                    pass
            self.pubsub = self.redis.pubsub()
        return self.pubsub

    # ============ WORKER STATE ============
    async def save_worker_state(self, worker_id: str, state: Dict[str, Any]):
        key = f"worker:{worker_id}:state"

        state["updated_at"] = datetime.now().isoformat()

        mapping = {}

        for k, v in state.items():
            if isinstance(v, (dict, list)):
                mapping[k] = json.dumps(v)
            else:
                mapping[k] = str(v) if v is not None else ""

        await self.redis.hset(key, mapping=mapping)
        await self.redis.expire(key, 86400 * 7)  # 7 days

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
        try:
            pubsub = await self.ensure_pubsub()
            await self.redis.publish(f"worker:{worker_id}:logs", json.dumps(log_entry))
        except Exception as e:
            self.logger.error(f"Error publishing log: {e}")

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
            except Exception:
                continue

        return parsed_logs

    # ============ PUB/SUB ============
    async def subscribe_to_logs(self, worker_id: str):
        """Subscribe to logs for a specific worker"""
        pubsub = await self.ensure_pubsub()
        await pubsub.subscribe(f"worker:{worker_id}:logs")
        return pubsub

    async def unsubscribe_from_logs(self, worker_id: str):
        """Unsubscribe from logs for a specific worker"""
        if self.pubsub:
            await self.pubsub.unsubscribe(f"worker:{worker_id}:logs")

    # ============ EVENT STORAGE ============
    async def store_event(
        self, worker_id: str, event_type: str, event_data: Dict[str, Any]
    ):
        """Store a structured event with specific type"""
        event = {
            "type": event_type,
            "timestamp": datetime.now().isoformat(),
            "worker_id": worker_id,
            "data": event_data,
        }

        # Store in event history (keep last 1000 events)
        history_key = f"worker:{worker_id}:events:history"
        await self.redis.lpush(history_key, json.dumps(event))
        await self.redis.ltrim(history_key, 0, 999)
        await self.redis.expire(history_key, 86400 * 7)  # 7 days

        # Store by event type for easy filtering
        type_key = f"worker:{worker_id}:events:{event_type}"
        await self.redis.lpush(type_key, json.dumps(event))
        await self.redis.ltrim(type_key, 0, 99)  # Keep last 100 per type
        await self.redis.expire(type_key, 86400 * 7)

        # Publish to real-time channel
        try:
            await self.redis.publish(f"worker:{worker_id}:events", json.dumps(event))
        except Exception as e:
            self.logger.error(f"Error publishing event: {e}")

        return event

    async def get_events(
        self,
        worker_id: str,
        event_type: Optional[str] = None,
        limit: int = 100,
        since: Optional[datetime] = None,
    ) -> List[Dict]:
        """Get events for a worker, optionally filtered by type"""
        if event_type:
            key = f"worker:{worker_id}:events:{event_type}"
        else:
            key = f"worker:{worker_id}:events:history"

        events = await self.redis.lrange(key, 0, limit - 1)

        parsed_events = []
        for event in events:
            try:
                entry = json.loads(event)
                if since:
                    event_time = datetime.fromisoformat(entry["timestamp"])
                    if event_time < since:
                        continue
                parsed_events.append(entry)
            except Exception:
                continue

        return parsed_events

    async def get_latest_event(self, worker_id: str, event_type: str) -> Optional[Dict]:
        """Get the latest event of a specific type"""
        key = f"worker:{worker_id}:events:{event_type}"
        event = await self.redis.lindex(key, 0)
        if event:
            return json.loads(event)
        return None

    async def get_all_event_types(self, worker_id: str) -> List[str]:
        """Get all event types for a worker"""
        pattern = f"worker:{worker_id}:events:*"
        keys = await self.redis.keys(pattern)
        # Filter out 'history' and other non-event-type keys
        return [key.split(":")[-1] for key in keys if key.split(":")[-1] != "history"]

    # ============ MONITORING METHODS ============
    async def get_worker_event_categories(self, worker_id: str) -> Dict[str, int]:
        """Get count of events by category for a worker"""
        events = await self.get_events(worker_id, limit=1000)
        categories = {}
        for event in events:
            category = event.get("data", {}).get("category", "unknown")
            categories[category] = categories.get(category, 0) + 1
        return categories

    async def get_worker_stats(self, worker_id: str) -> Dict[str, Any]:
        """Get comprehensive stats for a worker"""
        state = await self.get_worker_state(worker_id)
        events = await self.get_events(worker_id, limit=1000)

        # Parse config if it's a string
        config = {}
        if state and "config" in state:
            try:
                config = (
                    json.loads(state["config"])
                    if isinstance(state["config"], str)
                    else state["config"]
                )
            except:
                pass

        # Count event types
        event_types = {}
        for event in events:
            event_type = event.get("type", "unknown")
            event_types[event_type] = event_types.get(event_type, 0) + 1

        # Get latest events
        latest_events = events[:10] if events else []

        return {
            "worker_id": worker_id,
            "state": state,
            "config": config,
            "total_events": len(events),
            "event_types": event_types,
            "categories": await self.get_worker_event_categories(worker_id),
            "latest_events": latest_events,
            "is_running": state.get("status") == "running" if state else False,
        }

    async def get_worker_event_stream(
        self, worker_id: str, limit: int = 100
    ) -> List[Dict]:
        """Get events formatted for streaming display"""
        events = await self.get_events(worker_id, limit=limit)

        # Enhance events with display info
        enhanced_events = []
        for event in events:
            enhanced = {
                "id": event.get("_id", str(hash(json.dumps(event)))),
                "type": event.get("type", "unknown"),
                "timestamp": event.get("timestamp"),
                "worker_id": event.get("worker_id"),
                "data": event.get("data", {}),
                "display": {
                    "icon": self._get_event_icon(event.get("type", "unknown")),
                    "color": self._get_event_color(event.get("type", "unknown")),
                    "category": event.get("data", {}).get("category", "unknown"),
                    "message": event.get("data", {}).get(
                        "message", event.get("message", "")
                    ),
                },
            }
            enhanced_events.append(enhanced)

        return enhanced_events

    def _get_event_icon(self, event_type: str) -> str:
        """Get icon for event type"""
        icons = {
            "market_tick_analysis": "fa-chart-line",
            "trade_executed": "fa-exchange-alt",
            "trade_recorded": "fa-check-circle",
            "trade_resolved": "fa-flag-checkered",
            "trade_streak_confirmed": "fa-bolt",
            "system_heartbeat": "fa-heartbeat",
            "system_worker_start": "fa-play",
            "system_worker_booting": "fa-cog",
            "system_bot_cancelled": "fa-stop",
            "connection_streaming_connected": "fa-plug",
            "config_base_stake_set": "fa-sliders-h",
            "stats_updated": "fa-chart-bar",
            "stats_final_summary": "fa-flag",
            "martingale_prefetched": "fa-dice",
            "martingale_reset": "fa-undo",
            "worker_lifecycle": "fa-sync",
            "system_trade_kickoff": "fa-rocket",
            "system_trade_worker_started": "fa-play-circle",
            "connection_execution_ready": "fa-handshake",
            "connection_polling_ready": "fa-plug",
            "connection_tick_stream_subscribed": "fa-rss",
            "market_analyzing_market": "fa-search",
        }
        return icons.get(event_type, "fa-circle")

    def _get_event_color(self, event_type: str) -> str:
        """Get color for event type"""
        colors = {
            "market_tick_analysis": "blue",
            "trade_executed": "green",
            "trade_recorded": "emerald",
            "trade_resolved": "purple",
            "trade_streak_confirmed": "yellow",
            "system_heartbeat": "gray",
            "system_worker_start": "green",
            "system_worker_booting": "blue",
            "system_bot_cancelled": "red",
            "connection_streaming_connected": "cyan",
            "config_base_stake_set": "orange",
            "stats_updated": "indigo",
            "stats_final_summary": "purple",
            "martingale_prefetched": "pink",
            "martingale_reset": "red",
            "worker_lifecycle": "gray",
            "system_trade_kickoff": "amber",
            "system_trade_worker_started": "teal",
        }
        return colors.get(event_type, "gray")


# Global Redis instance
redis_manager = RedisManager()
