"""WebSocket data source implementation."""

import asyncio
import json
from typing import AsyncGenerator, Dict, Any, Optional

import aiohttp
from aiohttp import ClientWebSocketResponse, WSMsgType

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.base import Source


class WebSocketSource(Source):
    """WebSocket source for streaming real-time data."""

    def __init__(
        self,
        url: str,
        monitor: PipelineMonitor,
        reconnect_interval: float = 5.0,
        max_reconnect_attempts: int = 5,
    ):
        """
        Initialize WebSocket source.

        Args:
            url: WebSocket endpoint URL.
            monitor: Pipeline monitor instance.
            reconnect_interval: Seconds to wait between reconnection attempts.
            max_reconnect_attempts: Maximum number of reconnection attempts.
        """
        super().__init__(monitor)
        self.url = url
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[ClientWebSocketResponse] = None
        self._closed = False

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _connect_websocket(self) -> Optional[ClientWebSocketResponse]:
        """Connect to WebSocket with reconnection logic."""
        session = await self._get_session()

        for attempt in range(self.max_reconnect_attempts):
            try:
                self.monitor.log_event(f"Connecting to WebSocket: {self.url} (attempt {attempt + 1})")
                ws = await session.ws_connect(self.url)
                self.monitor.log_event(f"Successfully connected to WebSocket: {self.url}")
                return ws
            except aiohttp.ClientError as e:
                self.monitor.log_error(f"Failed to connect to WebSocket: {e}")
                if attempt < self.max_reconnect_attempts - 1:
                    self.monitor.log_event(f"Retrying connection in {self.reconnect_interval} seconds...")
                    await asyncio.sleep(self.reconnect_interval)
                else:
                    self.monitor.log_error(f"Maximum reconnection attempts reached for {self.url}")
                    return None

        return None

    async def _process_message(self, message: aiohttp.WSMessage) -> Optional[Dict[str, Any]]:
        """Process WebSocket message and convert to standard format."""
        if message.type == WSMsgType.TEXT:
            try:
                data = json.loads(message.data)

                # Handle different message formats
                if isinstance(data, dict):
                    if 'type' in data and data['type'] == 'tweet':
                        # Format: {"type": "tweet", "data": {...}}
                        return data['data']
                    elif 'data' in data:
                        # Format: {"data": {...}}
                        return data['data']
                    else:
                        # Plain data object
                        return data
                else:
                    self.monitor.log_warning(f"Unexpected data format: {data}")
                    return None

            except json.JSONDecodeError as e:
                self.monitor.log_error(f"Error decoding JSON from WebSocket: {e}")
                return None

        elif message.type == WSMsgType.ERROR:
            self.monitor.log_error(f"WebSocket error: {message.data}")
            return None

        elif message.type == WSMsgType.CLOSED:
            self.monitor.log_event("WebSocket connection closed")
            return None

        return None

    async def generate(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate data from WebSocket stream."""
        reconnect_attempts = 0

        while not self._closed:
            try:
                # Connect to WebSocket
                self._ws = await self._connect_websocket()
                if not self._ws:
                    break

                # Reset reconnect attempts after successful connection
                reconnect_attempts = 0

                # Process WebSocket messages
                async for message in self._ws:
                    # Process the received message
                    item = await self._process_message(message)

                    if item:
                        self.monitor.log_event(f"Received item from WebSocket {self.url}")
                        yield item

                # If we reach here, the WebSocket was closed
                self.monitor.log_event("WebSocket connection closed, attempting to reconnect...")

            except Exception as e:
                # Handle errors and implement reconnection logic
                self.monitor.log_error(f"Error in WebSocket connection: {e}")
                reconnect_attempts += 1

                if reconnect_attempts >= self.max_reconnect_attempts:
                    self.monitor.log_error(f"Max reconnection attempts reached ({reconnect_attempts})")
                    break

                self.monitor.log_event(f"Reconnecting in {self.reconnect_interval} seconds...")
                await asyncio.sleep(self.reconnect_interval)

            finally:
                # Close the current WebSocket connection if needed
                if self._ws and not self._ws.closed:
                    await self._ws.close()

        # Clean up resources
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
