"""Base class for all transformers."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from asyncdatapipeline.monitoring import PipelineMonitor


class BaseTransformer(ABC):
    """Abstract base class for all data transformers."""

    def __init__(self, monitor: PipelineMonitor):
        """
        Initialize the transformer with a monitor for logging and metrics.

        Args:
            monitor: Optional PipelineMonitor instance for tracking events and errors.
        """
        self.monitor = monitor

    @abstractmethod
    async def transform(self, data: Any) -> Any:
        """Transform data asynchronously."""
        pass

    async def __call__(self, data: Any) -> Any:
        """Make the transformer callable for pipeline compatibility."""
        try:
            return await self.transform(data)
        except Exception as e:
            self.monitor.log_error(f"Error in transformer {self.__class__.__name__}: {e}")
            raise

    def _get_key(self, data: Any, key: str = "text") -> Optional[str]:
        """Extract unique key from data."""
        if isinstance(data, dict):
            # Prefer tweet_id if available, else use Text
            return data.get(key, "")
        elif isinstance(data, str):
            return data
        self.monitor.log_debug(f"Data is not a dict or string: {data}")
        return None
