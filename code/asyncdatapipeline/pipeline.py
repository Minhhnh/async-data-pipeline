import asyncio
import json
import os
import ssl
from contextlib import asynccontextmanager
from time import time
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    List,
    Optional,
    Set,
    TypeVar,
)

import aiohttp
from asyncdatapipeline.config import PipelineConfig
from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.security import DataEncryptor

T = TypeVar('T')
SourceType = Callable[[], Any | asyncio.Future]
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
        # For recovery and checkpointing
        self.processed_ids: Set[str] = set()
        self.checkpoint_path = self.config.checkpoint_path

        # Initialize security components
        self.encryptor = None
        if self.config.enable_payload_encryption and self.config.encryption_key:
            self.encryptor = DataEncryptor(self.monitor, self.config.encryption_key)
            self.monitor.log_event("Payload encryption enabled")

    @asynccontextmanager
    async def _http_session(self) -> AsyncGenerator[aiohttp.ClientSession, None]:
        """Manage HTTP session with TLS support and certificate validation."""
        # Configure SSL context if TLS is enabled
        ssl_context = None
        if self.config.use_tls:
            ssl_context = ssl.create_default_context()

            # Configure SSL verification
            if not self.config.verify_ssl:
                self.monitor.log_warning("SSL certificate verification is disabled")
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

            # Use custom certificate if provided
            if self.config.ssl_cert_path and os.path.exists(self.config.ssl_cert_path):
                self.monitor.log_event(f"Using custom SSL certificate: {self.config.ssl_cert_path}")
                ssl_context.load_verify_locations(self.config.ssl_cert_path)

        # Create session with configured SSL context
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.session = session
            try:
                yield session
            finally:
                self.session = None

    async def _save_checkpoint(self) -> None:
        """Save processing state to enable recovery if interrupted."""
        if not self.checkpoint_path:
            return

        try:
            checkpoint_dir = os.path.dirname(self.checkpoint_path)
            if not os.path.exists(checkpoint_dir):
                os.makedirs(checkpoint_dir)

            checkpoint_data = {
                "processed_ids": list(self.processed_ids),
                "metrics": self.monitor.get_metrics()
            }

            # Write to a temporary file first, then rename to avoid corruption
            temp_path = f"{self.checkpoint_path}.tmp"
            with open(temp_path, 'w') as f:
                json.dump(checkpoint_data, f)

            # Atomic rename operation
            os.replace(temp_path, self.checkpoint_path)
            self.monitor.log_debug(f"Checkpoint saved to {self.checkpoint_path}")
        except Exception as e:
            self.monitor.log_error(f"Failed to save checkpoint: {e}")

    async def _load_checkpoint(self) -> None:
        """Load previous processing state for recovery."""
        if not self.checkpoint_path or not os.path.exists(self.checkpoint_path):
            return

        try:
            with open(self.checkpoint_path, 'r') as f:
                checkpoint_data = json.load(f)
                self.processed_ids = set(checkpoint_data.get("processed_ids", []))

            self.monitor.log_event(f"Loaded checkpoint with {len(self.processed_ids)} processed items")
        except Exception as e:
            self.monitor.log_error(f"Failed to load checkpoint: {e}")

    async def _process_source(self, source: SourceType) -> None:
        """Process a single source, apply transformations, and dispatch to destinations."""
        try:
            # Initialize source with retry if needed
            source_generator = None
            attempts = 0
            while attempts < self.config.retry_attempts:
                attempts += 1
                try:
                    source_generator = source()
                    break
                except Exception as e:
                    self.monitor.log_warning(
                        f"Source initialization failed (attempt {attempts}/{self.config.retry_attempts}): {e}"
                    )
                    if attempts < self.config.retry_attempts:
                        self.monitor.log_debug(f"Retrying in {self.config.retry_delay} seconds...")
                        await asyncio.sleep(self.config.retry_delay)
                    else:
                        self.monitor.log_error(f"Source failed after {self.config.retry_attempts} attempts")
                        raise

            # Process source data
            async for data in source_generator:
                # Generate a consistent ID for the data item
                data_id = str(hash(str(data)))

                # Skip already processed items if in recovery mode
                if data_id in self.processed_ids and self.config.enable_recovery:
                    self.monitor.log_debug(f"Skipping already processed item {data_id}")
                    continue

                start_time = time()
                processed_data = await self._apply_transformers(data)

                if processed_data is not None:  # Skip None from filters
                    await self._dispatch_to_destinations(processed_data)
                    # Mark as processed after successful processing
                    self.processed_ids.add(data_id)

                    # Save checkpoint periodically based on config setting
                    if (self.monitor.get_metrics()["throughput"] % self.config.checkpoint_frequency) == 0:
                        await self._save_checkpoint()

                self.monitor.track_processing(start_time)
        except Exception as e:
            self.monitor.log_error(f"Error processing source: {e}")
            # Try to save checkpoint on error to preserve progress
            await self._save_checkpoint()

    async def _apply_transformers(self, data: Any) -> Any:
        """Apply transformers sequentially with support for async transformers."""
        result = data
        for transformer in self.transformers:
            try:
                # Apply transformer with retry logic
                result = await self._apply_with_retry(
                    transformer,
                    result,
                    f"Transformer {getattr(transformer, '__name__', str(transformer))}"
                )

                if result is None:  # Early exit for filters
                    break
            except Exception as e:
                transformer_name = getattr(transformer, "__name__", str(transformer))
                self.monitor.log_error(f"Error in transformer {transformer_name}: {e}")
                raise
        return result

    async def _apply_with_retry(self, transformer: TransformerType, data: Any, component_name: str) -> Any:
        """Apply a function with retry logic."""
        attempts = 0
        last_exception = None

        while attempts < self.config.retry_attempts:
            attempts += 1
            try:
                # Apply transformer, supporting both async and non-async transformer functions
                transformed = transformer(data)
                if asyncio.iscoroutine(transformed):
                    return await transformed
                return transformed
            except Exception as e:
                last_exception = e
                self.monitor.log_warning(
                    f"{component_name} failed (attempt {attempts}/{self.config.retry_attempts}): {e}"
                )
                if attempts < self.config.retry_attempts:
                    self.monitor.log_debug(f"Retrying in {self.config.retry_delay} seconds...")
                    await asyncio.sleep(self.config.retry_delay)

        # If we get here, all retry attempts failed
        self.monitor.log_error(f"{component_name} failed after {self.config.retry_attempts} attempts")
        raise last_exception

    async def _dispatch_to_destinations(self, data: Any) -> None:
        """Dispatch data to destinations with concurrency control and optional encryption."""
        # Apply encryption if enabled
        if self.encryptor and self.config.enable_payload_encryption:
            data = self.encryptor.encrypt(data)

        async def try_destination(dest: DestinationType, data: Any) -> None:
            async with self.semaphore:
                # Apply destination with retry logic
                await self._apply_with_retry(
                    lambda d: dest(d),
                    data,
                    f"Destination {getattr(dest, '__name__', str(dest))}"
                )

        tasks = [try_destination(dest, data) for dest in self.destinations]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self) -> None:
        """Run the pipeline: collect, transform, and dispatch data."""
        # Load checkpoint for recovery if enabled
        if self.config.enable_recovery:
            await self._load_checkpoint()

        async with self._http_session():
            tasks = [self._process_source(source) for source in self.sources]
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                # Always save checkpoint at the end
                await self._save_checkpoint()
                self.monitor.log_event(f"Pipeline completed. Metrics: {self.monitor.get_metrics()}")
