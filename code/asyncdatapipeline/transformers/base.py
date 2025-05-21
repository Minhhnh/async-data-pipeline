"""Base class for all transformers."""

from abc import ABC, abstractmethod
from typing import Any, Dict

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

    def _get_key(self, data: Dict[str, Any]) -> str:
        """Generate a unique key for the data item.

        Args:
            data: The data item to generate a key for

        Returns:
            A string key for bloom filter
        """
        if not isinstance(data, dict):
            return str(data)

        # For dict data, create a key from relevant fields
        if 'id' in data:
            return str(data['id'])
        elif 'text' in data and 'username' in data:
            return f"{data['username']}:{data['text']}"
        else:
            # Create a key from all values, sorted by keys for consistency
            return ":".join(f"{k}={v}" for k, v in sorted(data.items()))
