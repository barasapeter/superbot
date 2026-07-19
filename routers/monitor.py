from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
import json
import os
from datetime import datetime
import math

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
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)
    except:
        return str(value)[:19] if value else "N/A"


def format_number(value):
    """Format number with commas and 2 decimal places"""
    if value is None:
        return "N/A"
    try:
        val = float(value)
        return f"{val:,.2f}"
    except (ValueError, TypeError):
        return str(value)


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
        "system_fetching_balance": "fa-spinner",
        "config_banner_displayed": "fa-flag",
        "stats_starting_balance": "fa-wallet",
        "error_critical_failure": "fa-exclamation-triangle",
        "error_fatal_error": "fa-skull",
        "system_bot_stopped": "fa-stop-circle",
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
        "system_fetching_balance": "blue",
        "config_banner_displayed": "orange",
        "stats_starting_balance": "cyan",
        "error_critical_failure": "red",
        "error_fatal_error": "red",
        "system_bot_stopped": "gray",
    }
    return colors.get(value, "gray")


def truncate_text(value, length=80):
    """Truncate text to specified length"""
    if not value:
        return ""
    if len(str(value)) > length:
        return str(value)[:length] + "..."
    return str(value)


def get_event_badge(event_type):
    """Get badge style for event type"""
    badges = {
        "trade_recorded": "bg-emerald-100 text-emerald-800",
        "trade_executed": "bg-blue-100 text-blue-800",
        "trade_resolved": "bg-purple-100 text-purple-800",
        "trade_streak_confirmed": "bg-yellow-100 text-yellow-800",
        "market_tick_analysis": "bg-indigo-100 text-indigo-800",
        "stats_updated": "bg-cyan-100 text-cyan-800",
        "stats_final_summary": "bg-purple-100 text-purple-800",
        "martingale_prefetched": "bg-pink-100 text-pink-800",
        "martingale_reset": "bg-red-100 text-red-800",
        "system_heartbeat": "bg-gray-100 text-gray-800",
        "system_bot_cancelled": "bg-red-100 text-red-800",
        "system_worker_start": "bg-green-100 text-green-800",
        "config_base_stake_set": "bg-orange-100 text-orange-800",
        "system_trade_kickoff": "bg-amber-100 text-amber-800",
        "system_trade_worker_started": "bg-teal-100 text-teal-800",
        "connection_created": "bg-cyan-100 text-cyan-800",
        "connection_execution_ready": "bg-green-100 text-green-800",
        "connection_polling_ready": "bg-green-100 text-green-800",
        "connection_streaming_connected": "bg-cyan-100 text-cyan-800",
        "connection_tick_stream_subscribed": "bg-cyan-100 text-cyan-800",
        "connection_connections_ready": "bg-green-100 text-green-800",
        "market_analyzing_market": "bg-indigo-100 text-indigo-800",
        "system_fetching_balance": "bg-blue-100 text-blue-800",
        "config_banner_displayed": "bg-orange-100 text-orange-800",
        "stats_starting_balance": "bg-cyan-100 text-cyan-800",
        "worker_lifecycle": "bg-gray-100 text-gray-800",
        "error_critical_failure": "bg-red-100 text-red-800",
        "error_fatal_error": "bg-red-100 text-red-800",
        "system_bot_stopped": "bg-gray-100 text-gray-800",
    }
    return badges.get(event_type, "bg-gray-100 text-gray-800")


def get_profit_color(profit):
    """Get color for profit value"""
    if profit is None:
        return "text-gray-500"
    try:
        p = float(profit)
        if p > 0:
            return "text-green-600"
        elif p < 0:
            return "text-red-600"
        return "text-gray-500"
    except:
        return "text-gray-500"


def format_currency(value, currency="USD"):
    """Format currency value"""
    if value is None:
        return "N/A"
    try:
        val = float(value)
        if val >= 0:
            return f"+{val:,.2f} {currency}"
        return f"{val:,.2f} {currency}"
    except:
        return str(value)


def get_trade_result_badge(result):
    """Get badge for trade result"""
    if result == "WON":
        return "bg-green-100 text-green-800"
    elif result == "LOST":
        return "bg-red-100 text-red-800"
    return "bg-gray-100 text-gray-800"


def get_streak_color(streak):
    """Get color for streak value"""
    if streak is None:
        return "text-gray-500"
    try:
        s = int(streak)
        if s > 0:
            return "text-green-600"
        elif s < 0:
            return "text-red-600"
        return "text-gray-500"
    except:
        return "text-gray-500"


# Register all filters - MAKE SURE formatNumber is registered
templates.env.filters["formatTimestamp"] = format_timestamp
templates.env.filters["formatNumber"] = format_number  # <-- This must be here
templates.env.filters["getEventIcon"] = get_event_icon
templates.env.filters["getEventColor"] = get_event_color
templates.env.filters["truncate"] = truncate_text
templates.env.filters["getEventBadge"] = get_event_badge
templates.env.filters["getProfitColor"] = get_profit_color
templates.env.filters["formatCurrency"] = format_currency
templates.env.filters["getTradeResultBadge"] = get_trade_result_badge
templates.env.filters["getStreakColor"] = get_streak_color

router = APIRouter(prefix="/monitor", tags=["monitor"])


def extract_latest_stats(events):
    """Extract the latest stats from events history"""
    pl = 0.0
    balance = 0.0
    win_rate = 0.0
    total_trades = 0
    wins = 0
    losses = 0

    # Find the latest stats_updated event (events are sorted newest first)
    for event in events:
        event_type = event.get("type", "")

        if event_type == "stats_updated":
            data = event.get("data", {})
            if "net_pl" in data:
                pl = float(data["net_pl"])
            if "current_balance" in data:
                balance = float(data["current_balance"])
            if "win_rate" in data:
                win_rate = float(data["win_rate"])
            if "total_trades" in data:
                total_trades = int(data["total_trades"])
            if "wins" in data:
                wins = int(data["wins"])
            if "losses" in data:
                losses = int(data["losses"])
            # Found the latest stats, break
            break

        elif event_type == "stats_final_summary":
            data = event.get("data", {})
            if "net_pl" in data:
                pl = float(data["net_pl"])
            if "final_balance" in data:
                balance = float(data["final_balance"])
            if "win_rate" in data:
                win_rate = float(data["win_rate"])
            if "total_trades" in data:
                total_trades = int(data["total_trades"])
            if "wins" in data:
                wins = int(data["wins"])
            if "losses" in data:
                losses = int(data["losses"])
            break

    return pl, balance, win_rate, total_trades, wins, losses


@router.get("/", response_class=HTMLResponse)
async def monitor_dashboard(request: Request):
    """Main monitoring dashboard"""
    all_workers = await redis_manager.get_all_workers()

    # Enhance worker data
    enhanced_workers = []
    total_pl = 0

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

        # Get events and extract stats
        events = await redis_manager.get_events(worker_id, limit=200)
        pl, balance, _, _, _, _ = extract_latest_stats(events)

        total_pl += pl

        # Get latest event for display
        latest_event = events[0] if events else None

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
                "pnl": pl,
                "balance": balance,
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
            "total_pl": total_pl,
        },
    )


@router.get("/worker/{worker_id}", response_class=HTMLResponse)
async def worker_detail(request: Request, worker_id: str):
    """Worker detail page with real-time streaming"""
    # Check if worker exists
    state = await redis_manager.get_worker_state(worker_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    # Get worker stats and events
    stats = await redis_manager.get_worker_stats(worker_id)

    # Get events - we need enough to find the latest stats
    events = await redis_manager.get_worker_event_stream(worker_id, limit=500)
    logs = await redis_manager.get_log_history(worker_id, limit=50)

    # Parse config
    config = stats.get("config", {})

    # Extract latest stats from events
    current_pl, current_balance, win_rate, total_trades, wins, losses = (
        extract_latest_stats(events)
    )

    # Fallback: try to get from state if still zero
    if current_balance == 0.0 and state:
        if "initial_balance" in config:
            current_balance = float(config.get("initial_balance", 0))

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
            "current_pl": current_pl,
            "current_balance": current_balance,
            "win_rate": win_rate,
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
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

        events = await redis_manager.get_events(worker_id, limit=200)
        pl, _, _, _, _, _ = extract_latest_stats(events)

        workers_list.append(
            {
                "worker_id": worker_id,
                "is_running": is_running,
                "status": worker_state.get("status", "unknown"),
                "started_at": worker_state.get("started_at"),
                "stopped_at": worker_state.get("stopped_at"),
                "pnl": pl,
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


@router.get("/api/worker/{worker_id}/pnl")
async def api_get_worker_pnl(worker_id: str):
    """API endpoint to get worker P/L history"""
    state = await redis_manager.get_worker_state(worker_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    events = await redis_manager.get_events(worker_id, limit=200)
    pl_history = []

    for event in events:
        if event.get("type") == "stats_updated":
            data = event.get("data", {})
            if "net_pl" in data:
                pl_history.append(
                    {
                        "timestamp": event.get("timestamp"),
                        "net_pl": float(data["net_pl"]),
                        "total_trades": int(data.get("total_trades", 0)),
                        "win_rate": float(data.get("win_rate", 0)),
                    }
                )

    return {
        "worker_id": worker_id,
        "pl_history": pl_history[::-1],
        "current_pl": pl_history[-1]["net_pl"] if pl_history else 0,
    }
