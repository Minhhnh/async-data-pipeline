"""API data source implementation."""

import asyncio
from typing import AsyncGenerator, Dict, Any, Optional

import aiohttp
from aiohttp import ClientSession

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.base import Source


class ApiSource(Source):
    """API source for fetching data from REST endpoints."""

    def __init__(
        self,
        url: str,
        monitor: PipelineMonitor,
    ):
        super().__init__(monitor)
        self.url = url
        self._session: Optional[ClientSession] = None

    async def _get_session(self) -> ClientSession:
        """Get or create an HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _fetch_from_rest(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Fetch data from REST API endpoint."""
        session = await self._get_session()

        try:
            while True:
                async with session.get(self.url) as response:
                    if response.status != 200:
                        self.monitor.log_error(f"Error fetching from {self.url}: {response.status}")
                        await asyncio.sleep(1)  # Backoff before retry
                        continue

                    data = await response.json()
                    self.monitor.log_event(f"Received item from {self.url}")
                    yield data

                # Add delay to avoid hammering the API
                await asyncio.sleep(0.5)
        except Exception as e:
            self.monitor.log_error(f"Error fetching from REST API {self.url}: {e}")
            raise

    async def generate(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate data from REST API."""
        try:
            self.monitor.log_event(f"Using REST API connection to {self.url}")
            async for item in self._fetch_from_rest():
                yield item
        except Exception as e:
            self.monitor.log_error(f"Error in ApiSource.generate for {self.url}: {e}")
            raise
        finally:
            # Clean up resources
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
