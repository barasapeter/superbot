import json
import logging
from typing import Dict, List, Any, Optional
import redis.asyncio as redis
from datetime import datetime

logger = logging.getLogger(__name__)

class RedisService:
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
    
    async def get_worker_events(self, worker_id: str) -> List[Dict[str, Any]]:
        """Fetch all events for a worker from Redis"""
        try:
            pattern = f"worker:{worker_id}:events:*"
            event_keys = await self.redis_client.keys(pattern)
            
            if not event_keys:
                return []
            
            all_events = []
            
            for key in event_keys:
                events = await self.redis_client.lrange(key, 0, -1)
                for event_json in events:
                    try:
                        event = json.loads(event_json)
                        all_events.append(event)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse event from {key}: {event_json}")
            
            # Sort by timestamp (newest first)
            all_events.sort(
                key=lambda x: x.get('timestamp', ''),
                reverse=True
            )
            
            return all_events
            
        except Exception as e:
            logger.error(f"Error fetching events for worker {worker_id}: {e}")
            raise
    
    async def get_worker_state(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get worker state from Redis"""
        state_key = f"worker:{worker_id}:state"
        try:
            state = await self.redis_client.hgetall(state_key)
            if not state:
                return None
            # Parse config if it exists
            if 'config' in state and isinstance(state['config'], str):
                try:
                    state['config'] = json.loads(state['config'])
                except:
                    pass
            return state
        except Exception as e:
            logger.error(f"Error fetching state for worker {worker_id}: {e}")
            return None
    
    async def get_worker_logs(self, worker_id: str) -> List[Dict[str, Any]]:
        """Fetch worker logs from Redis"""
        log_key = f"worker:{worker_id}:logs:history"
        try:
            logs = await self.redis_client.lrange(log_key, 0, -1)
            parsed_logs = []
            for log_json in logs:
                try:
                    log = json.loads(log_json)
                    parsed_logs.append(log)
                except:
                    pass
            parsed_logs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return parsed_logs
        except Exception as e:
            logger.error(f"Error fetching logs for worker {worker_id}: {e}")
            return []
    
    async def check_worker_exists(self, worker_id: str) -> bool:
        """Check if a worker exists in Redis"""
        try:
            pattern = f"worker:{worker_id}:*"
            keys = await self.redis_client.keys(pattern)
            return len(keys) > 0
        except Exception as e:
            logger.error(f"Error checking worker existence: {e}")
            return False
    
    async def get_all_workers(self) -> List[Dict[str, Any]]:
        """Get list of all workers with their states"""
        try:
            pattern = "worker:*:state"
            keys = await self.redis_client.keys(pattern)
            
            workers = []
            for key in keys:
                parts = key.split(':')
                if len(parts) >= 2:
                    worker_id = parts[1]
                    state = await self.get_worker_state(worker_id)
                    
                    # Determine if worker is running
                    is_running = False
                    if state:
                        status = state.get('status', '').lower()
                        is_running = status == 'running'
                    
                    workers.append({
                        "worker_id": worker_id,
                        "is_running": is_running,
                        "status": state.get('status') if state else 'unknown',
                        "started_at": state.get('started_at') if state else None,
                        "stopped_at": state.get('stopped_at') if state else None,
                        "symbol": json.loads(state.get('config', '{}')).get('symbol') if state and state.get('config') else None
                    })
            
            # Sort by started_at (newest first)
            workers.sort(
                key=lambda x: x.get('started_at', ''),
                reverse=True
            )
            
            return workers
        except Exception as e:
            logger.error(f"Error listing workers: {e}")
            return []
    
    async def is_worker_running(self, worker_id: str) -> bool:
        """Check if a worker is currently running"""
        state = await self.get_worker_state(worker_id)
        if not state:
            return False
        status = state.get('status', '').lower()
        return status == 'running'
    
    async def get_event_categories(self, worker_id: str) -> Dict[str, int]:
        """Get count of events by category for a worker"""
        events = await self.get_worker_events(worker_id)
        categories = {}
        for event in events:
            category = event.get('data', {}).get('category', 'unknown')
            categories[category] = categories.get(category, 0) + 1
        return categories
    
    async def get_recent_events(self, worker_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get most recent events for a worker"""
        events = await self.get_worker_events(worker_id)
        return events[:limit]