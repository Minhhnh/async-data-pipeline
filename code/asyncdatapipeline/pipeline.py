import asyncio
from contextlib import asynccontextmanager
from time import time
from typing import Any, AsyncGenerator, Callable, List, Optional, Coroutine, TypeVar

import aiohttp

from asyncdatapipeline.config import PipelineConfig
from asyncdatapipeline.monitoring import PipelineMonitor

T = TypeVar('T')
SourceType = Callable[[], AsyncGenerator[Any, None]]
TransformerType = Callable[[Any], Any]
DestinationType = Callable[[Any], Coroutine[Any, Any, None]]


class AsyncDataPipeline:
    """Asynchronous data pipeline for large-scale data processing with Twitter/X stream support."""

    def __init__(
        self,
        sources: List[SourceType],
        transformers: Optional[List[TransformerType]] = None,
        destinations: Optional[List[DestinationType]] = None,
        config: Optional[PipelineConfig] = None,
    ) -> None:
        """
        Initialize the pipeline.

        Args:
            sources: List of async generator functions for data collection (e.g., Twitter stream, files).
            transformers: List of transformation functions applied sequentially.
            destinations: List of async functions for data dispatch.
            config: Pipeline configuration (concurrent tasks, retries, Twitter credentials, etc.).
        """
        self.sources = sources
        self.transformers = transformers if transformers else []
        self.destinations = destinations if destinations else []
        self.config = config if config else PipelineConfig()
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent_tasks)
        self.monitor = PipelineMonitor()
        self.session: Optional[aiohttp.ClientSession] = None

    @asynccontextmanager
    async def _http_session(self) -> AsyncGenerator[aiohttp.ClientSession, None]:
        """Manage HTTP session with TLS support."""
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.config.use_tls)) as session:
            self.session = session
            try:
                yield session
            finally:
                self.session = None

    async def _process_source(self, source: SourceType) -> None:
        """Process a single source, apply transformations, and dispatch to destinations."""
        try:
            async for data in source():
                start_time = time()
                processed_data = await self._apply_transformers(data)
                if processed_data is not None:  # Skip None from filters
                    await self._dispatch_to_destinations(processed_data)
                self.monitor.track_processing(start_time)
        except Exception as e:
            self.monitor.log_error(f"Error processing source: {e}")

    async def _apply_transformers(self, data: Any) -> Any:
        """Apply transformers sequentially with support for async transformers."""
        result = data
        for transformer in self.transformers:
            try:
                # Apply transformer, supporting both async and non-async transformer functions
                transformed = transformer(result)
                if asyncio.iscoroutine(transformed):
                    result = await transformed
                else:
                    result = transformed

                if result is None:  # Early exit for filters
                    break
            except Exception as e:
                transformer_name = getattr(transformer, "__name__", str(transformer))
                self.monitor.log_error(f"Error in transformer {transformer_name}: {e}")
                raise
        return result

    async def _dispatch_to_destinations(self, data: Any) -> None:
        """Dispatch data to destinations with concurrency control."""

        async def try_destination(dest: DestinationType, data: Any) -> None:
            async with self.semaphore:
                await dest(data)

        tasks = [try_destination(dest, data) for dest in self.destinations]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self) -> None:
        """Run the pipeline: collect, transform, and dispatch data."""
        async with self._http_session():
            tasks = [self._process_source(source) for source in self.sources]
            await asyncio.gather(*tasks, return_exceptions=True)
            self.monitor.log_event(f"Pipeline completed. Metrics: {self.monitor.get_metrics()}")
