"""Base class for all data destinations."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Awaitable

from asyncdatapipeline.monitoring import PipelineMonitor


class Destination(ABC):
    """Abstract base class for all data destinations."""

    def __init__(self, monitor: PipelineMonitor):
        """
        Initialize the destination with a monitor for logging and metrics.

        Args:
            monitor: PipelineMonitor instance for tracking events and errors.
        """
        self.monitor = monitor

    @abstractmethod
    async def send(self, data: dict) -> None:
        """Send data to the destination asynchronously."""
        pass

    async def __call__(self, data: Any) -> None:
        """Make the destination callable for pipeline compatibility."""
        try:
            await self.send(data)
        except Exception as e:
            self.monitor.log_error(f"Error in destination {self.__class__.__name__}: {e}")
            raise
