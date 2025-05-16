"""Base class for all data sources."""

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator

from asyncdatapipeline.monitoring import PipelineMonitor


class Source(ABC):
    """Abstract base class for all data sources."""

    def __init__(self, monitor: PipelineMonitor):
        """
        Initialize the source with a monitor for logging and metrics.

        Args:
            monitor: PipelineMonitor instance for tracking events and errors.
        """
        self.monitor = monitor

    @abstractmethod
    async def generate(self) -> AsyncGenerator[Any, None]:
        """Generate data from the source asynchronously."""
        pass

    async def __call__(self) -> AsyncGenerator[Any, None]:
        """Make the source callable for pipeline compatibility."""
        try:
            async for item in self.generate():
                yield item
        except Exception as e:
            self.monitor.log_error(f"Error in source {self.__class__.__name__}: {e}")
            raise

    def __aiter__(self):
        """Make the source async iterable, returning an iterator for __call__."""
        return self.__call__().__aiter__()
