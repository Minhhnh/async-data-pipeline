"""Text transformers for data processing."""

import asyncio
from typing import Any, Dict, Optional

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.transformers.base import BaseTransformer
from rbloom import Bloom


class UppercaseTransformer(BaseTransformer):
    """Transforms string data to uppercase."""

    async def transform(self, data: Any) -> Any:
        """Transform input string to uppercase."""
        if not isinstance(data, str):
            self.monitor.log_debug(f"Data is not a string: {data}")
            return data
        result = data.upper()
        return result


class CSVToDictTransformer(BaseTransformer):
    """Transforms a CSV line to a dict object."""

    async def transform(self, line: Any) -> Optional[Dict[str, Any]]:
        """Transform CSV line to dictionary."""
        if isinstance(line, dict):
            self.monitor.log_debug(f"Line is already a dict: {line}")
            return line
        try:
            self.monitor.log_debug(f"Transforming line to dict: {line}")
            parts = line
            if isinstance(line, str):
                parts = line.strip().split(',')
            if len(parts) < 5:
                self.monitor.log_error(f"Invalid line format: {line}")
                return {}
            tweet_data = {
                "timestamp": parts[0],
                "username": parts[1],
                "text": parts[2],
                "created_at": parts[3],
                "retweets": parts[4],
                "likes": int(parts[5]),
            }
            return tweet_data
        except ValueError as e:
            self.monitor.log_error(f"Error transforming line to dict: {line} - {e}")
            return {}


class DeduplicateTransformer(BaseTransformer):
    """Removes duplicate data using a Bloom Filter."""

    def __init__(self, monitor: PipelineMonitor, capacity: int, error_rate: float):
        """
        Initialize the DeduplicateTransformer.

        Args:
            monitor: PipelineMonitor for logging.
            capacity: Expected number of elements (n).
            error_rate: Desired false positive rate (e.g., 0.01 for 1%).
        """
        super().__init__(monitor)
        try:
            self.bloom = Bloom(capacity, error_rate)
            self.monitor.log_event(f"Initialized Bloom Filter: capacity={capacity}, error_rate={error_rate}")
        except Exception as e:
            self.monitor.log_error(f"Failed to initialize Bloom Filter: {e}")
            raise

    async def transform(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter out duplicate data using Bloom Filter."""
        key = self._get_key(data)
        if not key:
            return data

        try:
            # Check and add to Bloom Filter asynchronously
            loop = asyncio.get_running_loop()

            def check_and_add() -> bool:
                in_filter = key in self.bloom
                if not in_filter:
                    self.bloom.add(key)
                return in_filter
            is_duplicate = await loop.run_in_executor(None, check_and_add)

            if is_duplicate:
                self.monitor.log_event(f"Filtered duplicate: {key[:50]}...")
                return None
            return data
        except Exception as e:
            self.monitor.log_error(f"DeduplicateTransformer error: {e}")
            raise
