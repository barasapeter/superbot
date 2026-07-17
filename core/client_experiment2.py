"""client_experiment2.py.py - Fixed with recv lock, timeout, and auto-reconnect"""

import json
import asyncio
import websockets
import time


class DerivClient:
    def __init__(self, ws_url):
        self.ws_url = ws_url
        self.ws = None
        self.req_id = 1
        self._recv_lock = asyncio.Lock()
        self._is_closing = False
        self._connected = False
        self._send_timeout = 10  # Default timeout for send operations

    async def connect(self):
        self.ws = await websockets.connect(self.ws_url, open_timeout=30)
        self._connected = True
        self._is_closing = False
        print("Connected")

    async def subscribe(self, payload):
        if self._is_closing or self.ws is None:
            raise Exception("Connection is closed")

        payload["req_id"] = self.req_id
        self.req_id += 1
        await self.ws.send(json.dumps(payload))

    async def send(self, payload, timeout=None):
        """
        Send a request and wait for the matching response with timeout.
        If timeout is None, uses default _send_timeout.
        """
        if self._is_closing or self.ws is None:
            raise Exception("Connection is closed")

        payload["req_id"] = self.req_id
        req_id = self.req_id
        self.req_id += 1

        await self.ws.send(json.dumps(payload))

        timeout = timeout or self._send_timeout
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Use asyncio.wait_for with a shorter timeout
                message = await asyncio.wait_for(self.recv(), timeout=2.0)
                if message.get("req_id") == req_id:
                    return message
            except asyncio.TimeoutError:
                # Check if connection is still alive
                if self.ws is None or self._is_closing:
                    raise Exception("Connection closed during receive")
                # Continue waiting
                continue
            except websockets.exceptions.ConnectionClosed as e:
                self._connected = False
                raise Exception(f"Connection closed during receive: {e}")
            except Exception as e:
                self._connected = False
                raise Exception(f"Receive error: {e}")

        raise Exception(f"Timeout waiting for response to request {req_id}")

    async def recv(self):
        """Receive a message with lock to prevent concurrent recv calls."""
        if self._is_closing or self.ws is None:
            raise Exception("Connection is closed")

        async with self._recv_lock:
            try:
                return json.loads(await asyncio.wait_for(self.ws.recv(), timeout=5.0))
            except asyncio.TimeoutError:
                raise Exception("recv timeout")
            except websockets.exceptions.ConnectionClosed:
                self._connected = False
                raise
            except Exception as e:
                self._connected = False
                raise

    async def recv_streaming(self):
        """For streaming responses where req_id matching is not needed."""
        if self._is_closing or self.ws is None:
            raise Exception("Connection is closed")

        async with self._recv_lock:
            try:
                return json.loads(await asyncio.wait_for(self.ws.recv(), timeout=5.0))
            except asyncio.TimeoutError:
                raise Exception("recv timeout")
            except websockets.exceptions.ConnectionClosed:
                self._connected = False
                raise
            except Exception as e:
                self._connected = False
                raise

    async def ping(self):
        """Send a ping to check connection health."""
        if self._is_closing or self.ws is None:
            return False
        try:
            await asyncio.wait_for(self.ws.ping(), timeout=2.0)
            return True
        except Exception:
            self._connected = False
            return False

    async def close(self):
        self._is_closing = True
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
            self._connected = False
        print("Disconnected")

    @property
    def is_connected(self):
        return self._connected and self.ws is not None and not self._is_closing
