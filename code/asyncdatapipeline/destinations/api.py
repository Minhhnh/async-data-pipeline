"""API destination implementation."""

from typing import Any, Dict, Optional

import aiohttp

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.destinations.base import Destination


class ApiDestination(Destination):
    """API destination for sending data to web endpoints."""

    def __init__(
        self,
        url: str,
        monitor: PipelineMonitor,
        headers: Optional[Dict[str, str]] = None,
        method: str = "POST"
    ):
        super().__init__(monitor)
        self.url = url
        self.headers = headers or {}
        self.method = method.upper()

    async def send(self, data: Any) -> None:
        """Send data to API endpoint asynchronously."""
        try:
            async with aiohttp.ClientSession() as session:
                if self.method == "POST":
                    async with session.post(self.url, json={"data": data}, headers=self.headers) as resp:
                        if resp.status >= 400:
                            raise ValueError(f"API error: {resp.status}")
                        self.monitor.log_debug(f"Sent data to {self.url}, status: {resp.status}")
                elif self.method == "PUT":
                    async with session.put(self.url, json={"data": data}, headers=self.headers) as resp:
                        if resp.status >= 400:
                            raise ValueError(f"API error: {resp.status}")
                        self.monitor.log_debug(f"Sent data to {self.url}, status: {resp.status}")
        except Exception as e:
            self.monitor.log_error(f"Error sending data to API {self.url}: {e}")
            raise
