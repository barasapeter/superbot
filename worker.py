"""Fixed Martingale Pre-Fetch with Dual Connections & WebSocket Recovery - Worker Version"""

import asyncio
import os
import logging
import sys
import time
import json
from datetime import datetime
from dotenv import load_dotenv
from core.auth import get_ws_url
from disposables.client_experiment2 import DerivClient
from typing import Dict, Any
from redis_manager import redis_manager

# worker.py - Add event storage function


async def store_event(
    worker_id: str, event_type: str, event_data: Dict[str, Any], logger=None
):
    """Store event in Redis and log it"""
    try:
        from redis_manager import redis_manager

        event = await redis_manager.store_event(worker_id, event_type, event_data)
        if logger:
            logger.info(
                f"📝 Event stored: {event_type} - {event_data.get('message', '')}"
            )
        return event
    except Exception as e:
        if logger:
            logger.error(f"Failed to store event: {e}")
        return None


# ==================== TERMINAL COLORS ====================
class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"

    GREEN = "\033[38;5;46m"
    RED = "\033[38;5;196m"
    YELLOW = "\033[38;5;220m"
    CYAN = "\033[38;5;51m"
    BLUE = "\033[38;5;33m"
    MAGENTA = "\033[38;5;201m"
    GREY = "\033[38;5;244m"
    WHITE = "\033[38;5;255m"
    ORANGE = "\033[38;5;208m"
    PINK = "\033[38;5;205m"
    PURPLE = "\033[38;5;129m"

    @staticmethod
    def pl(value):
        color = C.GREEN if value > 0 else C.RED if value < 0 else C.GREY
        sign = "+" if value > 0 else ""
        return f"{color}{sign}{value:.2f}{C.RESET}"


class ColorFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime("%H:%M:%S.%f")[:-3]

    def format(self, record):
        base_color = self.LEVEL_COLORS.get(record.levelno, C.WHITE)
        timestamp = f"{C.GREY}{self.formatTime(record, '%H:%M:%S.%f')[:-3]}{C.RESET}"
        level = f"{base_color}{record.levelname:<8}{C.RESET}"
        message = record.getMessage()
        return f"{timestamp} {level} {message}"


ColorFormatter.LEVEL_COLORS = {
    logging.DEBUG: C.GREY,
    logging.INFO: C.WHITE,
    logging.WARNING: C.YELLOW,
    logging.ERROR: C.RED,
    logging.CRITICAL: C.RED + C.BOLD,
}


# ==================== SESSION P&L TRACKER ====================
class SessionStats:
    def __init__(self, initial_balance=15.0, currency="USD"):
        self.initial_balance = initial_balance
        self.currency = currency
        self.net_pl = 0.0
        self.wins = 0
        self.losses = 0
        self.trades = 0
        self.start_time = datetime.now()
        self.trade_history = []
        self.pending_trades = {}
        self.consecutive_losses = 0
        self.max_consecutive_losses = 0
        self.max_drawdown = 0.0
        self.peak_balance = initial_balance

    def get_current_balance(self):
        return self.initial_balance + self.net_pl

    def get_drawdown_percent(self):
        current = self.get_current_balance()
        if self.peak_balance > 0:
            return ((self.peak_balance - current) / self.peak_balance) * 100
        return 0.0

    def record(self, profit, contract_id, signal, stake, entry_price, exit_price):
        self.net_pl += profit
        self.trades += 1
        if profit > 0:
            self.wins += 1
            self.consecutive_losses = 0
        else:
            self.losses += 1
            self.consecutive_losses += 1
            if self.consecutive_losses > self.max_consecutive_losses:
                self.max_consecutive_losses = self.consecutive_losses

        current_balance = self.get_current_balance()
        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
        drawdown = self.get_drawdown_percent()
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown

        self.trade_history.append(
            {
                "time": datetime.now().isoformat(),
                "contract_id": contract_id,
                "signal": signal,
                "stake": stake,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "profit": profit,
                "result": "WON" if profit > 0 else "LOST",
                "balance": current_balance,
            }
        )

    def summary_line(self):
        win_rate = (self.wins / self.trades * 100) if self.trades else 0.0
        pl_color = C.GREEN if self.net_pl >= 0 else C.RED
        active = (
            f" | {C.YELLOW}Active: {len(self.pending_trades)}{C.RESET}"
            if self.pending_trades
            else ""
        )
        dd = (
            f" | {C.RED}DD: {self.max_drawdown:.1f}%{C.RESET}"
            if self.max_drawdown > 0
            else ""
        )
        return (
            f"{C.BOLD}📊 SESSION{C.RESET} | Trades: {C.CYAN}{self.trades}{C.RESET} "
            f"| Wins: {C.GREEN}{self.wins}{C.RESET} | Losses: {C.RED}{self.losses}{C.RESET} "
            f"| Win Rate: {C.CYAN}{win_rate:.1f}%{C.RESET} "
            f"| Net P/L: {pl_color}{C.BOLD}{self.net_pl:+.2f} {self.currency}{C.RESET}"
            f"{active}{dd}"
        )

    def detailed_summary(self):
        if not self.trade_history:
            return "No trades executed."

        lines = [
            f"\n{C.BOLD}{C.CYAN}═══════════════════════════════════════════════════════════{C.RESET}",
            f"{C.BOLD}📊 SESSION DETAILED SUMMARY{C.RESET}",
            f"{C.GREY}Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}{C.RESET}",
            f"{C.GREY}Duration: {datetime.now() - self.start_time}{C.RESET}",
            "",
            f"{C.BOLD}Performance:{C.RESET}",
            f"  Trades: {self.trades}  Wins: {C.GREEN}{self.wins}{C.RESET}  Losses: {C.RED}{self.losses}{C.RESET}  Win Rate: {C.CYAN}{(self.wins/self.trades*100):.1f}%{C.RESET}",
            f"  Net P/L: {C.pl(self.net_pl)} {self.currency}",
            f"  Current Balance: {C.WHITE}{self.get_current_balance():.2f} {self.currency}{C.RESET}",
            f"  Max Drawdown: {C.RED}{self.max_drawdown:.1f}%{C.RESET}",
            f"  Max Consecutive Losses: {C.RED}{self.max_consecutive_losses}{C.RESET}",
            "",
            f"{C.BOLD}{C.UNDERLINE}Trade History:{C.RESET}",
        ]

        for i, trade in enumerate(self.trade_history, 1):
            result_color = C.GREEN if trade["result"] == "WON" else C.RED
            lines.append(
                f"  #{i:2d} {trade['time'][11:19]} {trade['signal']:4s} "
                f"Stake: {trade['stake']:5.2f}  Entry: {trade['entry_price']:.3f}  "
                f"Exit: {trade['exit_price']:.3f}  {result_color}{trade['result']:4s}{C.RESET}  "
                f"P/L: {C.pl(trade['profit'])}  Bal: {trade['balance']:.2f}"
            )

        lines.append(
            f"{C.CYAN}═══════════════════════════════════════════════════════════{C.RESET}"
        )
        return "\n".join(lines)


# ==================== MARTINGALE MANAGER ====================
class MartingaleManager:
    def __init__(
        self, base_stake, multiplier=2.0, enabled=True, max_steps=6, max_stake=5000
    ):
        self.base_stake = base_stake
        self.multiplier = multiplier
        self.enabled = enabled
        self.max_steps = max_steps
        self.max_stake = max_stake
        self.current_stake = base_stake
        self.step = 0
        self.loss_streak = 0
        self._lock = asyncio.Lock()
        self._pending_stake = None
        self._is_prefetched = False
        self._stopped = False
        self.logger = None

    def set_logger(self, logger):
        self.logger = logger

    async def get_current_stake(self):
        async with self._lock:
            return self.current_stake

    async def next_stake_async(self):
        async with self._lock:
            if self._stopped:
                if self.logger:
                    self.logger.warning(
                        f"{C.RED}❌ Martingale stopped - max steps reached!{C.RESET}"
                    )
                return None

            if self._pending_stake is not None:
                stake = self._pending_stake
                self._pending_stake = None
                self._is_prefetched = False
                return round(stake, 2)
            return (
                round(self.current_stake, 2)
                if self.current_stake <= self.max_stake
                else self.max_stake
            )

    async def pre_fetch_next_stake(self, currency="USD"):
        async with self._lock:
            if not self.enabled or self._stopped:
                return

            if self.step >= self.max_steps:
                self._stopped = True
                if self.logger:
                    stop_warning = f"{C.RED}🛑 Martingale stopped after {self.max_steps} steps!{C.RESET}"
                    self.logger.warning(stop_warning)
                return

            next_stake = round(self.current_stake * self.multiplier, 2)
            if next_stake > self.max_stake:
                if self.logger:
                    cap_warning = f"{C.YELLOW}Martingale stake of {next_stake} exceeds maximum. Capping new stake at {self.max_stake} {currency}{C.RESET}"
                    self.logger.warning(cap_warning)
                next_stake = self.max_stake
            self.current_stake = next_stake
            self._pending_stake = next_stake
            self._is_prefetched = True
            self.step += 1
            self.loss_streak += 1

            if self.logger:
                remaining = self.max_steps - self.step
                self.logger.info(
                    f"{C.ORANGE}📈 Martingale pre-fetched step {self.step}/{self.max_steps}{C.RESET} — "
                    f"next stake ready: {C.ORANGE}{C.BOLD}{next_stake} {currency}{C.RESET} "
                    f"{C.GREY}(loss streak: {self.loss_streak}, {remaining} steps remaining){C.RESET}"
                )

    async def record_result_async(self, won, currency="USD"):
        if not self.enabled:
            return

        async with self._lock:
            if won:
                if self.step > 0 or self._is_prefetched:
                    if self.logger:
                        self.logger.info(
                            f"{C.GREEN}🔁 Martingale reset{C.RESET} — win recovered the drawdown, "
                            f"back to base stake ({C.BOLD}{self.base_stake} {currency}{C.RESET})."
                        )
                self.current_stake = self.base_stake
                self.step = 0
                self.loss_streak = 0
                self._pending_stake = None
                self._is_prefetched = False
                self._stopped = False
                return

            self._pending_stake = None
            self._is_prefetched = False

            if self.step >= self.max_steps:
                self._stopped = True
                if self.logger:
                    self.logger.warning(
                        f"{C.RED}🛑 Martingale reached max steps ({self.max_steps})!{C.RESET}"
                    )

    def status_tag(self):
        if not self.enabled:
            return f"{C.GREY}[Flat Stake]{C.RESET}"
        if self._stopped:
            return f"{C.RED}[STOPPED]{C.RESET}"
        if self._is_prefetched or self._pending_stake is not None:
            return f"{C.ORANGE}[Pre-fetched x{self.step}]{C.RESET}"
        if self.step == 0:
            return f"{C.GREEN}[Base]{C.RESET}"
        return f"{C.ORANGE}[Martingale x{self.step}]{C.RESET}"


# ==================== PERSISTENT TRADE MANAGER ====================
class PersistentTradeManager:
    def __init__(self, config, logger, stats, martingale):
        self.config = config
        self.logger = logger
        self.stats = stats
        self.martingale = martingale
        self.execution_client = None
        self.polling_client = None
        self.lock = asyncio.Lock()
        self.pending_trades = asyncio.Queue()
        self.last_trade_time = 0
        self.heartbeat_task = None
        self.reconnect_attempts = 0
        self._is_closing = False
        self._trade_lock = asyncio.Lock()
        self._pending_contracts = {}
        self._heartbeat_failures = 0

    async def _create_client(self, label="client"):
        try:
            ws_url = get_ws_url(
                account_type=self.config.get("account_type", "demo"),
                token=self.config["api_token"],
                app_id=self.config.get("app_id", "1089"),
            )
            client = DerivClient(ws_url)
            await client.connect()
            return client
        except Exception as e:
            self.logger.error(f"{C.RED}❌ Failed to create {label}: {e}{C.RESET}")
            return None

    async def ensure_execution_client(self):
        if self._is_closing:
            return None

        if self.execution_client is None or not self.execution_client.is_connected:
            try:
                if self.execution_client:
                    await self.execution_client.close()
                self.execution_client = await self._create_client("execution")
                if self.execution_client:
                    self.logger.info(f"{C.GREEN}✅ Execution client ready.{C.RESET}")
                    self.reconnect_attempts = 0
            except Exception as e:
                self.logger.error(f"{C.RED}❌ Execution client failed: {e}{C.RESET}")
                self.reconnect_attempts += 1
                if self.reconnect_attempts >= self.config.get(
                    "max_reconnect_attempts", 5
                ):
                    self.logger.error(
                        f"{C.RED}❌ Max reconnect attempts reached.{C.RESET}"
                    )
                    return None
                return None

        return self.execution_client

    async def ensure_polling_client(self):
        if self._is_closing:
            return None

        if self.polling_client is None or not self.polling_client.is_connected:
            try:
                if self.polling_client:
                    await self.polling_client.close()
                self.polling_client = await self._create_client("polling")
                if self.polling_client:
                    self.logger.info(f"{C.GREEN}✅ Polling client ready.{C.RESET}")
                    self.reconnect_attempts = 0
            except Exception as e:
                self.logger.error(f"{C.RED}❌ Polling client failed: {e}{C.RESET}")
                self.reconnect_attempts += 1
                if self.reconnect_attempts >= self.config.get(
                    "max_reconnect_attempts", 5
                ):
                    self.logger.error(
                        f"{C.RED}❌ Max reconnect attempts reached.{C.RESET}"
                    )
                    return None
                return None

        return self.polling_client

    async def heartbeat(self):
        consecutive_failures = 0
        max_consecutive_failures = 3

        while not self._is_closing:
            await asyncio.sleep(self.config.get("heartbeat_interval", 5))
            if self._is_closing:
                break

            try:
                exec_ok = True
                if self.execution_client and self.execution_client.ws:
                    exec_ok = await self.execution_client.ping()
                    if not exec_ok:
                        self.logger.warning(
                            f"{C.YELLOW}⚠️ Execution client ping failed, reconnecting...{C.RESET}"
                        )
                        self.execution_client = None
                        await self.ensure_execution_client()

                poll_ok = True
                if self.polling_client and self.polling_client.ws:
                    poll_ok = await self.polling_client.ping()
                    if not poll_ok:
                        self.logger.warning(
                            f"{C.YELLOW}⚠️ Polling client ping failed, reconnecting...{C.RESET}"
                        )
                        self.polling_client = None
                        await self.ensure_polling_client()

                if exec_ok and poll_ok:
                    consecutive_failures = 0
                    self._heartbeat_failures = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_failures += 1
                self._heartbeat_failures += 1
                self.logger.warning(
                    f"{C.YELLOW}⚠️ Heartbeat warning ({consecutive_failures}): {str(e)[:50]}{C.RESET}"
                )

                if consecutive_failures >= max_consecutive_failures:
                    self.logger.warning(
                        f"{C.YELLOW}🔄 Multiple heartbeat failures, forcing reconnect...{C.RESET}"
                    )
                    self.execution_client = None
                    self.polling_client = None
                    await self.ensure_execution_client()
                    await self.ensure_polling_client()
                    consecutive_failures = 0

    async def execute_trade_instant(self, signal, stake, symbol, currency):
        start_time = time.time()

        client = await self.ensure_execution_client()
        if client is None:
            return None, None

        proposal_payload = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": signal,
            "currency": currency,
            "duration": self.config.get("contract_duration", 5),
            "duration_unit": "t",
            "underlying_symbol": symbol,
        }

        try:
            proposal_start = time.time()
            proposal_res = await client.send(proposal_payload)
            proposal_time = (time.time() - proposal_start) * 1000

            if "error" in proposal_res:
                self.logger.error(
                    f"{C.RED}❌ Proposal failed: {proposal_res['error'].get('message')}{C.RESET}"
                )
                return None, None

            proposal_data = proposal_res.get("proposal", {})
            proposal_id = proposal_data.get("id")

            entry_price = proposal_data.get("entry_spot")
            if entry_price is None:
                entry_price = proposal_data.get("spot")
            if entry_price is None:
                entry_price = proposal_data.get("quote")
            if entry_price is None:
                entry_price = 0.0

            if not proposal_id:
                return None, None

            buy_start = time.time()
            buy_payload = {"buy": proposal_id, "price": stake}
            buy_res = await client.send(buy_payload)
            buy_time = (time.time() - buy_start) * 1000

            if "error" in buy_res:
                self.logger.error(
                    f"{C.RED}❌ Purchase failed: {buy_res['error'].get('message')}{C.RESET}"
                )
                return None, None

            contract_id = buy_res.get("buy", {}).get("contract_id")
            total_time = (time.time() - start_time) * 1000

            self.logger.info(
                f"{C.GREEN}✅ Trade executed!{C.RESET} Contract: {C.CYAN}{contract_id}{C.RESET} "
                f"{C.GREY}| Entry: {C.WHITE}{entry_price:.3f}{C.RESET} "
                f"{C.GREY}| Latency: Proposal {proposal_time:.0f}ms + Buy {buy_time:.0f}ms = {total_time:.0f}ms{C.RESET}"
            )

            return contract_id, entry_price

        except asyncio.TimeoutError:
            self.logger.error(
                f"{C.RED}❌ Trade execution timeout - connection may be dead.{C.RESET}"
            )
            self.execution_client = None
            return None, None
        except Exception as e:
            self.logger.error(f"{C.RED}❌ Trade execution error: {e}{C.RESET}")
            self.execution_client = None
            return None, None

    async def poll_contract_status(self, contract_id):
        client = await self.ensure_polling_client()
        if client is None:
            self.logger.error(f"{C.RED}❌ No polling client available.{C.RESET}")
            return None, None

        start_time = time.time()
        poll_count = 0
        consecutive_errors = 0

        while time.time() - start_time < self.config.get("poll_timeout", 25):
            if self._is_closing:
                return None, None

            try:
                response = await client.send(
                    {"proposal_open_contract": 1, "contract_id": contract_id}
                )
                poll_count += 1
                consecutive_errors = 0

                if "error" in response:
                    error_msg = response["error"].get("message", "unknown")
                    self.logger.warning(
                        f"{C.YELLOW}⚠️ Poll {poll_count} API error: {error_msg}{C.RESET}"
                    )
                    await asyncio.sleep(0.5)
                    continue

                poc = response.get("proposal_open_contract", {})
                if poc.get("is_sold"):
                    status = poc.get("status", "unknown").upper()
                    profit = float(poc.get("profit", 0.0))
                    exit_price = float(poc.get("exit_spot", 0.0))

                    emoji = (
                        "🏆" if status == "WON" else "❌" if status == "LOST" else "⏳"
                    )
                    status_color = (
                        C.GREEN
                        if status == "WON"
                        else C.RED if status == "LOST" else C.YELLOW
                    )

                    self.logger.info(
                        f"{emoji} {C.BOLD}CONTRACT {contract_id} RESULT:{C.RESET} "
                        f"{status_color}{status}{C.RESET} {C.GREY}|{C.RESET} "
                        f"Exit: {C.WHITE}{exit_price:.3f}{C.RESET} {C.GREY}|{C.RESET} "
                        f"Profit: {C.pl(profit)} {self.config.get('currency', 'USD')} {C.GREY}(polls: {poll_count}){C.RESET}"
                    )
                    return profit, exit_price

            except asyncio.TimeoutError:
                consecutive_errors += 1
                self.logger.warning(
                    f"{C.YELLOW}⚠️ Poll {poll_count+1} timed out (error {consecutive_errors}){C.RESET}"
                )
                if consecutive_errors >= 2:
                    self.logger.warning(
                        f"{C.YELLOW}🔄 Reconnecting polling client...{C.RESET}"
                    )
                    self.polling_client = None
                    client = await self.ensure_polling_client()
                    if client is None:
                        self.logger.error(
                            f"{C.RED}❌ Failed to reconnect polling client.{C.RESET}"
                        )
                        return None, None
                    consecutive_errors = 0
                await asyncio.sleep(0.5)

            except asyncio.CancelledError:
                break

            except Exception as e:
                consecutive_errors += 1
                error_msg = str(e)[:100]
                self.logger.warning(
                    f"{C.YELLOW}⚠️ Status poll error ({consecutive_errors}): {error_msg}{C.RESET}"
                )

                if consecutive_errors >= 2:
                    self.logger.warning(
                        f"{C.YELLOW}🔄 Reconnecting polling client...{C.RESET}"
                    )
                    self.polling_client = None
                    client = await self.ensure_polling_client()
                    if client is None:
                        self.logger.error(
                            f"{C.RED}❌ Failed to reconnect polling client.{C.RESET}"
                        )
                        return None, None
                    consecutive_errors = 0
                await asyncio.sleep(1.0)

            await asyncio.sleep(self.config.get("poll_interval", 0.3))

        self.logger.warning(
            f"{C.YELLOW}⏱️ Contract {contract_id} timed out after {self.config.get('poll_timeout', 25)} seconds ({poll_count} polls).{C.RESET}"
        )
        return None, None

    async def process_trade(self, signal, symbol, currency):
        if self._is_closing:
            return

        if self.martingale._stopped:
            self.logger.warning(
                f"{C.RED}❌ Martingale stopped - no more trades allowed.{C.RESET}"
            )
            return

        current_time = time.time()
        if current_time - self.last_trade_time < self.config.get("cooldown_seconds", 6):
            remaining = self.config.get("cooldown_seconds", 6) - (
                current_time - self.last_trade_time
            )
            self.logger.info(
                f"{C.GREY}⏳ Cooldown active ({remaining:.1f}s remaining){C.RESET}"
            )
            return

        drawdown = self.stats.get_drawdown_percent()
        if drawdown > self.config.get("stop_loss_percent", 100):
            self.logger.warning(
                f"{C.RED}🛑 Stop loss triggered! Drawdown: {drawdown:.1f}% > {self.config.get('stop_loss_percent', 100)}%{C.RESET}"
            )
            return

        stake = await self.martingale.next_stake_async()

        if stake is None:
            self.logger.warning(
                f"{C.RED}❌ No stake available - martingale stopped.{C.RESET}"
            )
            return

        async with self._trade_lock:
            self.last_trade_time = time.time()

            sig_color = C.GREEN if signal == "CALL" else C.RED
            self.logger.info(
                f"{C.PURPLE}⚡ EXECUTING{C.RESET} {sig_color}{signal}{C.RESET} at "
                f"{C.BOLD}{stake} {currency}{C.RESET} {self.martingale.status_tag()}"
            )

            contract_id, entry_price = await self.execute_trade_instant(
                signal, stake, symbol, currency
            )

            if not contract_id:
                self.logger.error(
                    f"{C.RED}❌ Failed to execute {signal} trade.{C.RESET}"
                )
                return

            self.stats.pending_trades[contract_id] = {
                "signal": signal,
                "stake": stake,
                "entry": entry_price,
            }

            await self.martingale.pre_fetch_next_stake(currency)

            asyncio.create_task(
                self._resolve_trade(contract_id, signal, stake, entry_price, currency)
            )

    async def _resolve_trade(self, contract_id, signal, stake, entry_price, currency):
        profit, exit_price = await self.poll_contract_status(contract_id)

        self.stats.pending_trades.pop(contract_id, None)

        if profit is not None:
            self.stats.record(
                profit, contract_id, signal, stake, entry_price, exit_price
            )
            self.logger.info(self.stats.summary_line())

            await self.martingale.record_result_async(won=profit > 0, currency=currency)
        else:
            self.logger.error(
                f"{C.RED}❌ Could not determine outcome for contract {contract_id}{C.RESET}"
            )
            await self.martingale.record_result_async(won=False, currency=currency)
            async with self.martingale._lock:
                self.martingale.current_stake = self.martingale.base_stake
                self.martingale.step = 0
                self.martingale.loss_streak = 0
                self.martingale._pending_stake = None
                self.martingale._is_prefetched = False
                self.martingale._stopped = False
            self.logger.info(
                f"{C.YELLOW}🔄 Martingale reset due to unresolved contract.{C.RESET}"
            )

    async def trade_worker(self):
        self.heartbeat_task = asyncio.create_task(self.heartbeat())
        self.logger.info(
            f"{C.GREEN}💓 Heartbeat started (interval: {self.config.get('heartbeat_interval', 5)}s){C.RESET}"
        )

        while not self._is_closing:
            try:
                signal, symbol, currency = await self.pending_trades.get()
                await self.process_trade(signal, symbol, currency)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"{C.RED}❌ Trade worker error: {e}{C.RESET}")
                import traceback

                traceback.print_exc()
            finally:
                self.pending_trades.task_done()

    def queue_trade(self, signal, symbol, currency):
        if self._is_closing:
            return
        if self.martingale._stopped:
            self.logger.warning(
                f"{C.RED}❌ Cannot queue trade - martingale stopped.{C.RESET}"
            )
            return
        self.pending_trades.put_nowait((signal, symbol, currency))

    async def close(self):
        self._is_closing = True

        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except Exception:
                pass

        if self.execution_client:
            try:
                await self.execution_client.close()
                self.logger.info(f"{C.GREY}🔌 Execution connection closed.{C.RESET}")
            except Exception:
                pass

        if self.polling_client:
            try:
                await self.polling_client.close()
                self.logger.info(f"{C.GREY}🔌 Polling connection closed.{C.RESET}")
            except Exception:
                pass


# ==================== TICK STREAK TRACKER ====================
class TickStreakTracker:
    def __init__(
        self,
        target_streak=4,
        max_allowed_latency_ms=350,
        signal_cooldown=1.0,
        worker_id=None,
    ):
        self.prices = []
        self.timestamps = []
        self.streak = 0
        self.target_streak = target_streak
        self.max_latency = max_allowed_latency_ms
        self.clock_drift_warning_triggered = False
        self.last_signal_time = 0
        self.signal_cooldown = signal_cooldown
        self.logger = None
        self.worker_id = worker_id

    def set_logger(self, logger):
        self.logger = logger

    async def process_new_tick(
        self, price, server_epoch, martingale, stats, currency="USD"
    ):
        server_epoch_ms = int(server_epoch * 1000)
        local_time_ms = int(time.time() * 1000)
        latency = local_time_ms - server_epoch_ms

        if abs(latency) > 10000 and not self.clock_drift_warning_triggered:
            if self.logger:
                self.logger.warning(
                    f"🚨 {C.YELLOW}SYSTEM CLOCK OUT OF SYNC{C.RESET}: Your local time differs from the "
                    f"server by {C.BOLD}{latency/1000:.1f}s{C.RESET}."
                )
                await redis_manager.store_event(
                    worker_id=self.worker_id,
                    event_type="system_clock_out_of_sync",
                    event_data={
                        "type": "logline",
                        "level": "error",
                        "desc": f"SYSTEM CLOCK OUT OF SYNC. System time differs from the server by {latency/1000:.1f}s",
                    },
                )
            self.clock_drift_warning_triggered = True

        if latency > self.max_latency:
            if self.logger:
                self.logger.warning(
                    f"{C.YELLOW}⚠️ Skipping tick ({C.ORANGE}High Latency: {latency}ms{C.RESET})"
                )
                await redis_manager.store_event(
                    worker_id=self.worker_id,
                    event_type="skipping_tick",
                    event_data={
                        "type": "logline",
                        "level": "neutral",
                        "desc": f"Skipping tick (High Latency: {latency}ms)",
                    },
                )
            return "SKIP_LATENCY"

        if len(self.prices) > 0:
            last_price = self.prices[-1]
            if price > last_price:
                self.streak = self.streak + 1 if self.streak > 0 else 1
            elif price < last_price:
                self.streak = self.streak - 1 if self.streak < 0 else -1
            else:
                self.streak = 0

        self.prices.append(price)
        self.timestamps.append(time.time())
        if len(self.prices) > 20:
            self.prices.pop(0)
            self.timestamps.pop(0)

        if self.streak > 0:
            direction_emoji, dir_color = "📈", C.GREEN
        elif self.streak < 0:
            direction_emoji, dir_color = "📉", C.RED
        else:
            direction_emoji, dir_color = "➡️", C.GREY

        streak_meter = self._streak_meter(dir_color)
        latency_color = C.GREEN if latency < self.max_latency * 0.5 else C.YELLOW

        tick_freq = "N/A"
        if len(self.timestamps) > 1:
            avg_interval = (self.timestamps[-1] - self.timestamps[0]) / (
                len(self.timestamps) - 1
            )
            tick_freq = f"{avg_interval*1000:.0f}ms"

        stake = martingale.current_stake
        if martingale._pending_stake is not None:
            stake = martingale._pending_stake

        # Live tick analysis output - this is the key status display
        if self.logger:
            self.logger.info(
                f"{dir_color}●{C.RESET} Price: {C.WHITE}{C.BOLD}{price:.3f}{C.RESET}  "
                f"Streak: {dir_color}{self.streak:+d}{C.RESET} {direction_emoji} {streak_meter}  "
                f"{C.GREY}│{C.RESET} Latency: {latency_color}{latency}ms{C.RESET} "
                f"{C.GREY}({tick_freq}){C.RESET}  "
                f"{C.GREY}│{C.RESET} Next Stake: {C.WHITE}{stake:.2f} {currency}{C.RESET} {martingale.status_tag()}  "
                f"{C.GREY}│{C.RESET} {stats.summary_line()}"
            )
            await redis_manager.store_event(
                worker_id=self.worker_id,
                event_type="status",
                event_data={
                    "type": "banner",
                    "level": "info",
                    "title": "Tick Analysis",
                    "desc": f"{dir_color}●{C.RESET} Price: {C.WHITE}{C.BOLD}{price:.3f}{C.RESET}  "
                    f"Streak: {dir_color}{self.streak:+d}{C.RESET} {direction_emoji} {streak_meter}  "
                    f"{C.GREY}│{C.RESET} Latency: {latency_color}{latency}ms{C.RESET} "
                    f"{C.GREY}({tick_freq}){C.RESET}  "
                    f"{C.GREY}│{C.RESET} Next Stake: {C.WHITE}{stake:.2f} {currency}{C.RESET} {martingale.status_tag()}  "
                    f"{C.GREY}│{C.RESET} {stats.summary_line()}",
                },
            )

        current_time = time.time()
        if current_time - self.last_signal_time < self.signal_cooldown:
            return "HOLD"

        if self.streak >= self.target_streak:
            self.last_signal_time = current_time
            return "PUT"
        elif self.streak <= -self.target_streak:
            self.last_signal_time = current_time
            return "CALL"

        return "HOLD"

    def _streak_meter(self, dir_color):
        filled = min(abs(self.streak), self.target_streak)
        empty = self.target_streak - filled
        return f"[{dir_color}{'█' * filled}{C.RESET}{C.GREY}{'░' * empty}{C.RESET}]"


# ==================== BANNER ====================
async def print_banner(config, martingale, worker_id=None, logger=None):
    mg_line = (
        f"{C.GREY}Martingale:{C.RESET} {C.GREEN}ON{C.RESET} (x{config['martingale_multiplier']}, max {config['max_martingale_steps']} steps)"
        if config.get("martingale_enabled", True)
        else f"{C.GREY}Martingale:{C.RESET} {C.RED}OFF{C.RESET} (flat stake)"
    )
    banner = f"""
{C.CYAN}{C.BOLD}══════════════════════════════════════════════════════════
           TICK-STREAK BOT                   
══════════════════════════════════════════════════════════{C.RESET}
{C.GREY}  Symbol:{C.RESET} {C.WHITE}{config['symbol']}{C.RESET}   {C.GREY}Base Stake:{C.RESET} {C.WHITE}{config['base_stake']} {config['currency']}{C.RESET}   {C.GREY}Target Streak:{C.RESET} {C.WHITE}{config['target_streak']}{C.RESET}   {C.GREY}Duration:{C.RESET} {C.WHITE}{config['contract_duration']} ticks{C.RESET}
  {mg_line}
  {C.GREY}Stop Loss:{C.RESET} {C.WHITE}{config.get('stop_loss_percent', 100)}% drawdown{C.RESET}
  {C.GREEN}⚡ FIXED: Martingale Pre-Fetch | Dual Connections | WebSocket Recovery{C.RESET}
  {C.GREY}💓 Heartbeat: {config.get('heartbeat_interval', 5)}s | Max Reconnect: {config.get('max_reconnect_attempts', 5)}{C.RESET}
"""
    print(banner)

    # Store banner event if we have worker_id and logger
    if worker_id:

        event_data = {
            "message": "Banner displayed",
            "symbol": config["symbol"],
            "base_stake": config["base_stake"],
            "currency": config["currency"],
            "target_streak": config["target_streak"],
            "martingale_enabled": config.get("martingale_enabled", True),
            "martingale_multiplier": config.get("martingale_multiplier", 2.0),
            "max_martingale_steps": config.get("max_martingale_steps", 7),
        }

        await redis_manager.store_event(worker_id, "banner_displayed", event_data)
        print(
            "🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆 Worker ID found! Storing event... 🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆🏆"
        )
    else:
        print(
            "❌❌❌❌❌❌❌❌❌❌❌❌❌❌ Event not persisted!!! ❌❌❌❌❌❌❌❌❌❌❌❌❌❌"
        )


# ==================== HELPER FUNCTIONS ====================
async def get_account_balance(client):
    res = await client.send({"balance": 1})
    if "error" in res:
        return 0.0
    return float(res.get("balance", {}).get("balance", 0.0))


# ==================== MAIN WORKER FUNCTION ====================
async def run_worker(config, logger=None, worker_id=None):
    """
    Main worker function that accepts all parameters via config dict.

    Required config keys:
    - api_token: str
    - symbol: str
    - currency: str
    - base_stake: float (will be overridden by balance percentage if set)
    - initial_stake_percentage: float (percentage of balance to use as base stake)
    - target_streak: int
    - contract_duration: int
    - cooldown_seconds: int
    - max_latency_ms: int
    - martingale_enabled: bool
    - martingale_multiplier: float
    - max_martingale_steps: int
    - max_stake: float
    - heartbeat_interval: int
    - max_reconnect_attempts: int
    - reconnect_delay: int
    - stop_loss_percent: float
    - poll_timeout: int
    - poll_interval: float
    - app_id: str (optional, default "1089")
    - account_type: str (optional, default "demo")
    - min_stake: float (optional, default 0.35)
    """

    # Setup logging
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColorFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    logger = logging.getLogger(f"DerivWorker_{config.get('symbol', 'R_100')}")

    # Extract config with defaults
    api_token = config.get("api_token")
    if not api_token:
        logger.error("Execution stopped: Missing API token in config.")
        return

    symbol = config.get("symbol", "R_100")
    currency = config.get("currency", "USD")
    app_id = config.get("app_id", "1089")
    account_type = config.get("account_type", "demo")
    min_stake = config.get("min_stake", 0.35)
    initial_stake_percentage = config.get("initial_stake_percentage", 0.5)
    target_streak = config.get("target_streak", 4)
    contract_duration = config.get("contract_duration", 5)
    cooldown_seconds = config.get("cooldown_seconds", 6)
    max_latency_ms = config.get("max_latency_ms", 350)
    martingale_enabled = config.get("martingale_enabled", True)
    martingale_multiplier = config.get("martingale_multiplier", 2.0)
    max_martingale_steps = config.get("max_martingale_steps", 7)
    max_stake = config.get("max_stake", 5000)
    heartbeat_interval = config.get("heartbeat_interval", 5)
    max_reconnect_attempts = config.get("max_reconnect_attempts", 5)
    stop_loss_percent = config.get("stop_loss_percent", 100)
    poll_timeout = config.get("poll_timeout", 25)
    poll_interval = config.get("poll_interval", 0.3)
    signal_cooldown = config.get("signal_cooldown", 1.0)

    # ============ ADD THIS CODE HERE ============
    # Get worker_id from config or generate one
    if not worker_id:
        worker_id = config.get(
            "worker_id", f"worker_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

    # Store initial events
    if logger:
        await store_event(
            worker_id,
            "worker_start",
            {
                "message": "Worker starting",
                "symbol": symbol,
                "currency": currency,
                "base_stake": config.get("base_stake"),
                "config": {
                    k: v for k, v in config.items() if k != "api_token"
                },  # Don't store sensitive data
            },
            logger,
        )
    # ============ END OF ADDED CODE ============

    # beginning of client stream data...
    from redis_manager import redis_manager

    logger.info(
        f"{C.GREY}🚀 Starting worker {worker_id} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{C.RESET}"
    )

    await redis_manager.store_event(
        worker_id=worker_id,
        event_type="worker_booting",
        event_data={
            "type": "logline",
            "level": "info",
            "desc": f"Worker thread {worker_id} started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}",
        },
    )

    # Fetch balance
    logger.info(f"{C.CYAN}💰 Fetching live account balance...{C.RESET}")
    await redis_manager.store_event(
        worker_id=worker_id,
        event_type="fetching_balance",
        event_data={
            "type": "logline",
            "level": "info",
            "desc": "Fetching live account balance...",
        },
    )
    try:
        balance_client = DerivClient(
            ws_url=get_ws_url(account_type=account_type, token=api_token, app_id=app_id)
        )
        await balance_client.connect()
        initial_balance = await get_account_balance(balance_client)
        await balance_client.close()
    except Exception as e:
        logger.error(f"{C.RED}❌ Failed to fetch balance: {e}{C.RESET}")
        await redis_manager.store_event(
            worker_id=worker_id,
            event_type="fetching_balance_failed",
            event_data={
                "type": "logline",
                "level": "error",
                "desc": f"Failed to fetch balance: {e}.",
            },
        )
        return

    if initial_balance <= 0:
        logger.error(f"{C.RED}❌ Invalid or zero balance returned — aborting.{C.RESET}")
        await redis_manager.store_event(
            worker_id=worker_id,
            event_type="invalid_or_zero_balance_returned",
            event_data={
                "type": "logline",
                "level": "error",
                "desc": "Invalid or zero balance returned. Aborting...",
            },
        )
        return

    # Calculate base stake from balance percentage
    base_stake = round(initial_balance * initial_stake_percentage / 100, 2)
    if base_stake < min_stake:
        logger.warning(
            f"Calculated base stake from balance of {initial_balance} {currency} is {base_stake} {currency}. "
            f"Resetting to absolute minimum {min_stake} {currency}... Please consider recharging your balance."
        )
        base_stake = min_stake
        await redis_manager.store_event(
            worker_id=worker_id,
            event_type="invalid_or_zero_balance_returned",
            event_data={
                "type": "banner",
                "level": "warning",
                "title": "Low Funds",
                "desc": f"Calculated base stake from balance of {initial_balance} {currency} is {base_stake} {currency}. "
                f"Resetting to absolute minimum {min_stake} {currency}... Please consider recharging your balance.",
            },
        )

    full_config = {
        "api_token": api_token,
        "symbol": symbol,
        "currency": currency,
        "app_id": app_id,
        "account_type": account_type,
        "base_stake": base_stake,
        "min_stake": min_stake,
        "initial_stake_percentage": initial_stake_percentage,
        "target_streak": target_streak,
        "contract_duration": contract_duration,
        "cooldown_seconds": cooldown_seconds,
        "max_latency_ms": max_latency_ms,
        "martingale_enabled": martingale_enabled,
        "martingale_multiplier": martingale_multiplier,
        "max_martingale_steps": max_martingale_steps,
        "max_stake": max_stake,
        "heartbeat_interval": heartbeat_interval,
        "max_reconnect_attempts": max_reconnect_attempts,
        "stop_loss_percent": stop_loss_percent,
        "poll_timeout": poll_timeout,
        "poll_interval": poll_interval,
        "signal_cooldown": signal_cooldown,
    }

    logger.info(
        f"{C.CYAN}📐 Base stake set to {initial_stake_percentage}% of balance: {C.BOLD}{base_stake} {currency}{C.RESET}"
    )
    await redis_manager.store_event(
        worker_id=worker_id,
        event_type="base_stake_set",
        event_data={
            "type": "logline",
            "level": "info",
            "desc": f"Base stake set to {initial_stake_percentage}% of balance ({base_stake} {currency})",
        },
    )

    # Initialize components
    stats = SessionStats(initial_balance=initial_balance, currency=currency)
    logger.info(
        f"{C.GREY}💰 Starting Balance: {stats.initial_balance:.2f} {currency}{C.RESET}"
    )
    await redis_manager.store_event(
        worker_id=worker_id,
        event_type="base_stake_set",
        event_data={
            "type": "logline",
            "level": "info",
            "desc": f"Starting Balance: {stats.initial_balance:.2f} {currency}",
        },
    )

    martingale = MartingaleManager(
        base_stake=base_stake,
        multiplier=martingale_multiplier,
        enabled=martingale_enabled,
        max_steps=max_martingale_steps,
        max_stake=max_stake,
    )
    martingale.set_logger(logger)

    await print_banner(full_config, martingale, worker_id=worker_id)
    await redis_manager.store_event(
        worker_id=worker_id,
        event_type="trade_kickoff",
        event_data={
            "type": "banner",
            "level": "info",
            "title": "Trade has kicked off",
            "desc": {
                "symbol": full_config.get("symbol"),
                "base_stake": full_config.get("base_stake"),
                "target_streak": full_config.get("target_streak"),
                "stop_loss": round(stop_loss_percent / 100 * initial_balance, 2),
            },
        },
    )

    trade_manager = PersistentTradeManager(full_config, logger, stats, martingale)

    logger.info("🔌 Pre-connecting trading connections...")
    await trade_manager.ensure_execution_client()
    await trade_manager.ensure_polling_client()
    logger.info(f"{C.GREEN}✅ Trading connections ready.{C.RESET}")

    worker_task = asyncio.create_task(trade_manager.trade_worker())
    logger.info("🚀 Trade worker started.")

    # Connect to tick stream
    ws_url_ticks = get_ws_url(account_type=account_type, token=api_token, app_id=app_id)
    tick_client = DerivClient(ws_url_ticks)

    try:
        await tick_client.connect()
        logger.info(
            f"{C.GREEN}✅ Streaming connection successfully established.{C.RESET}"
        )

        logger.info(f"Subscribing to tick stream for {C.CYAN}{symbol}{C.RESET}...")
        await tick_client.ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))

        tracker = TickStreakTracker(
            target_streak=target_streak,
            max_allowed_latency_ms=max_latency_ms,
            signal_cooldown=signal_cooldown,
            worker_id=worker_id,
        )
        tracker.set_logger(logger)

        last_signal_time = 0

        logger.info(
            f"{C.CYAN}👁️ Now analyzing market... Awaiting {C.BOLD}{target_streak}{C.RESET} tick streak on {C.CYAN}{symbol}{C.RESET}."
        )
        await redis_manager.store_event(
            worker_id=worker_id,
            event_type="tick_stream_subscription",
            event_data={
                "type": "banner",
                "level": "info",
                "title": "Subscribed to tick stream",
                "desc": {
                    "trading_connections_pre_connected": "Pre-connected trading connections",
                    "trade_worker_started": "Trade worker has spawned.",
                    "streaming_connection_established": "Streaming connection successfully established.",
                    "tick_stream_subscribed": f"Subscribed to tick stream for {symbol}",
                },
            },
        )
        await redis_manager.store_event(
            worker_id=worker_id,
            event_type="analyzing_market",
            event_data={
                "type": "logline",
                "level": "info",
                "desc": f"Now analyzing market... Awaiting {target_streak} tick streak on {symbol}.",
            },
        )

        async for message_str in tick_client.ws:
            message = json.loads(message_str)

            if message.get("msg_type") == "tick":
                tick_data = message.get("tick", {})
                price = float(tick_data.get("quote"))
                epoch = float(tick_data.get("epoch"))

                # Process tick and get live analysis output
                signal = await tracker.process_new_tick(
                    price, epoch, martingale, stats, currency
                )

                current_time = time.time()
                if current_time - last_signal_time < cooldown_seconds:
                    continue

                if signal in ["CALL", "PUT"]:
                    sig_color = C.GREEN if signal == "CALL" else C.RED
                    logger.info(
                        f"{C.YELLOW}🔥 Strike Streak Confirmed!{C.RESET} Triggering {sig_color}{C.BOLD}{signal}{C.RESET} order at price {C.WHITE}{price:.3f}{C.RESET}"
                    )

                    await redis_manager.store_event(
                        worker_id=worker_id,
                        event_type="strike_streak_confirmed",
                        event_data={
                            "type": "banner",
                            "level": "crimson",
                            "title": "Streak Confirmed!",
                            "desc": f"Strike Streak Confirmed! Triggering {signal} order at price {price:.3f}",
                        },
                    )

                    trade_manager.queue_trade(signal, symbol, currency)
                    last_signal_time = current_time
                    tracker.streak = 0

            elif "error" in message:
                logger.error(
                    f"{C.RED}❌ WebSocket incoming error: {message['error'].get('message')}{C.RESET}"
                )
                await redis_manager.store_event(
                    worker_id=worker_id,
                    event_type="websocket_incoming_error",
                    event_data={
                        "type": "logline",
                        "level": "error",
                        "desc": f"WebSocket incoming error: {message['error'].get('message')}",
                    },
                )

    except asyncio.CancelledError:
        logger.info("Bot execution cancelled. Shutting down gracefully...")
        await redis_manager.store_event(
            worker_id=worker_id,
            event_type="bot_execution_cancelled",
            event_data={
                "type": "logline",
                "level": "crimson",
                "desc": "Bot execution cancelled. Shutting down gracefully...",
            },
        )
    except Exception as e:
        logger.error(f"{C.RED}❌ Critical failure: {e}{C.RESET}", exc_info=True)
        await redis_manager.store_event(
            worker_id=worker_id,
            event_type="bot_execution_cancelled",
            event_data={
                "type": "logline",
                "level": "error",
                "desc": f"Critical failure: {e}",
            },
        )
    finally:
        logger.info("Tearing down active connections...")
        logger.info(stats.detailed_summary())

        worker_task.cancel()
        try:
            await worker_task
        except Exception:
            pass

        if tick_client and tick_client.ws is not None:
            await tick_client.close()

        await trade_manager.close()
        logger.info(
            f"{C.GREY}🛑 Bot stopped at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{C.RESET}"
        )
        await redis_manager.store_event(
            worker_id=worker_id,
            event_type="final_words",
            event_data={
                "type": "banner",
                "level": "crimson",
                "title": "Final teardown",
                "desc": {
                    "teardown": "Active connections teared down",
                    "worker_termination": f"Bot stopped at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                },
            },
        )

        return stats


# ==================== MAIN ENTRY POINT ====================
if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Example configuration - all values can be passed from environment or external source
    config = {
        "api_token": os.getenv("TOKEN"),
        "symbol": os.getenv("SYMBOL", "R_100"),
        "currency": os.getenv("CURRENCY", "USD"),
        "app_id": os.getenv("APP_ID", "1089"),
        "account_type": os.getenv("ACCOUNT_TYPE", "demo"),
        "min_stake": float(os.getenv("MIN_STAKE", "0.35")),
        "initial_stake_percentage": float(os.getenv("INITIAL_STAKE_PERCENTAGE", "0.5")),
        "target_streak": int(os.getenv("TARGET_STREAK", "4")),
        "contract_duration": int(os.getenv("CONTRACT_DURATION", "5")),
        "cooldown_seconds": int(os.getenv("COOLDOWN_SECONDS", "6")),
        "max_latency_ms": int(os.getenv("MAX_LATENCY_MS", "350")),
        "martingale_enabled": os.getenv("MARTINGALE_ENABLED", "true").lower() == "true",
        "martingale_multiplier": float(os.getenv("MARTINGALE_MULTIPLIER", "2.0")),
        "max_martingale_steps": int(os.getenv("MAX_MARTINGALE_STEPS", "7")),
        "max_stake": float(os.getenv("MAX_STAKE", "5000")),
        "heartbeat_interval": int(os.getenv("HEARTBEAT_INTERVAL", "5")),
        "max_reconnect_attempts": int(os.getenv("MAX_RECONNECT_ATTEMPTS", "5")),
        "stop_loss_percent": float(os.getenv("STOP_LOSS_PERCENT", "100")),
        "poll_timeout": int(os.getenv("POLL_TIMEOUT", "25")),
        "poll_interval": float(os.getenv("POLL_INTERVAL", "0.3")),
        "signal_cooldown": float(os.getenv("SIGNAL_COOLDOWN", "1.0")),
    }

    try:
        asyncio.run(run_worker(config, logger=None))
    except KeyboardInterrupt:
        print("\nBot manually terminated. Goodbye!")
