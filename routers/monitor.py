from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
import json
import os
from datetime import datetime

from redis_manager import redis_manager
from persistent_worker import workers

# Setup templates
templates_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
templates = Jinja2Templates(directory=templates_dir)


# Add custom filters to templates
def format_timestamp(value):
    """Format timestamp for display"""
    if not value:
        return "N/A"
    try:
        if isinstance(value, str):
            # Try to parse ISO format
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)
    except:
        return str(value)[:19] if value else "N/A"


def get_event_icon(value):
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
    return icons.get(value, "fa-circle")


def get_event_color(value):
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
    return colors.get(value, "gray")


def truncate_text(value, length=80):
    """Truncate text to specified length"""
    if not value:
        return ""
    if len(str(value)) > length:
        return str(value)[:length] + "..."
    return str(value)


# Register filters
templates.env.filters["formatTimestamp"] = format_timestamp
templates.env.filters["getEventIcon"] = get_event_icon
templates.env.filters["getEventColor"] = get_event_color
templates.env.filters["truncate"] = truncate_text

router = APIRouter(prefix="/monitor", tags=["monitor"])


@router.get("/", response_class=HTMLResponse)
async def monitor_dashboard(request: Request):
    """Main monitoring dashboard"""
    all_workers = await redis_manager.get_all_workers()

    # Enhance worker data
    enhanced_workers = []
    for worker_state in all_workers:
        worker_id = worker_state.get("worker_id")
        is_running = worker_id in workers and workers[worker_id].is_running

        # Parse config
        config = {}
        if "config" in worker_state:
            try:
                config = (
                    json.loads(worker_state["config"])
                    if isinstance(worker_state["config"], str)
                    else worker_state["config"]
                )
            except:
                pass

        # Get latest event
        latest_events = await redis_manager.get_events(worker_id, limit=1)
        latest_event = latest_events[0] if latest_events else None

        enhanced_workers.append(
            {
                "worker_id": worker_id,
                "is_running": is_running,
                "status": worker_state.get("status", "unknown"),
                "started_at": worker_state.get("started_at"),
                "stopped_at": worker_state.get("stopped_at"),
                "symbol": config.get("symbol", "N/A"),
                "config": config,
                "latest_event": latest_event,
                "last_heartbeat": worker_state.get("last_heartbeat"),
            }
        )

    # Sort by started_at (newest first)
    enhanced_workers.sort(key=lambda x: x.get("started_at", ""), reverse=True)

    return templates.TemplateResponse(
        "monitor/index.html",
        {
            "request": request,
            "workers": enhanced_workers,
            "total_workers": len(enhanced_workers),
            "running_workers": sum(1 for w in enhanced_workers if w["is_running"]),
        },
    )


@router.get("/worker/{worker_id}", response_class=HTMLResponse)
async def worker_detail(request: Request, worker_id: str):
    """Worker detail page with real-time streaming"""
    # Check if worker exists
    state = await redis_manager.get_worker_state(worker_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    # Get worker stats
    stats = await redis_manager.get_worker_stats(worker_id)
    events = await redis_manager.get_worker_event_stream(worker_id, limit=100)
    logs = await redis_manager.get_log_history(worker_id, limit=50)

    # Parse config
    config = stats.get("config", {})

    # Get WebSocket URL
    ws_url = (
        f"ws://{request.headers.get('host', 'localhost:8080')}/monitor/ws/{worker_id}"
    )

    return templates.TemplateResponse(
        "monitor/worker_detail.html",
        {
            "request": request,
            "worker_id": worker_id,
            "state": state,
            "config": config,
            "is_running": stats.get("is_running", False),
            "stats": stats,
            "events": events,
            "logs": logs,
            "total_events": stats.get("total_events", 0),
            "event_types": stats.get("event_types", {}),
            "categories": stats.get("categories", {}),
            "ws_url": ws_url,
        },
    )


@router.get("/api/workers")
async def api_list_workers():
    """API endpoint to list all workers with stats"""
    all_workers = await redis_manager.get_all_workers()

    workers_list = []
    for worker_state in all_workers:
        worker_id = worker_state.get("worker_id")
        is_running = worker_id in workers and workers[worker_id].is_running

        workers_list.append(
            {
                "worker_id": worker_id,
                "is_running": is_running,
                "status": worker_state.get("status", "unknown"),
                "started_at": worker_state.get("started_at"),
                "stopped_at": worker_state.get("stopped_at"),
            }
        )

    return {"total": len(workers_list), "workers": workers_list}


@router.get("/api/worker/{worker_id}/events")
async def api_get_worker_events(
    worker_id: str, limit: int = 100, event_type: Optional[str] = None
):
    """API endpoint to get worker events"""
    state = await redis_manager.get_worker_state(worker_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    events = await redis_manager.get_events(worker_id, event_type, limit)
    return {"worker_id": worker_id, "total": len(events), "events": events}


@router.get("/api/worker/{worker_id}/stats")
async def api_get_worker_stats(worker_id: str):
    """API endpoint to get worker stats"""
    state = await redis_manager.get_worker_state(worker_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    stats = await redis_manager.get_worker_stats(worker_id)
    return stats
