"""File destination implementation for async data pipeline."""

import os
from typing import Any

import aiofiles

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.destinations.base import Destination
from aiocsv import AsyncWriter
import json


class FileDestination(Destination):
    """File destination for writing data to local files."""

    def __init__(self, file_path: str, monitor: PipelineMonitor, encoding: str = "utf-8"):
        super().__init__(monitor)
        self._file_path = file_path
        self._encoding = encoding
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self._file_path), exist_ok=True)

    async def send(self, data: dict) -> None:
        """Write data to file asynchronously."""
        try:
            async with aiofiles.open(self._file_path, mode="a", encoding=self._encoding) as f:
                await f.write(",".join(map(str, data.values())) + "\n")
        except Exception as e:
            self.monitor.log_error(f"Error writing to file {self._file_path}: {e}")
            raise


class CSVFileDestination(FileDestination):
    """CSV file destination for writing data to CSV files."""

    def __init__(self, file_path: str, monitor: PipelineMonitor, encoding: str = "utf-8"):
        super().__init__(file_path, monitor, encoding)

    async def send(self, data: dict) -> None:
        """Write data to CSV file asynchronously."""
        try:
            async with aiofiles.open(self._file_path, mode="a", encoding=self._encoding) as f:
                writer = AsyncWriter(f)
                await writer.writerow(data.values())
        except Exception as e:
            self.monitor.log_error(f"Error writing to file {self._file_path}: {e}")
            raise


class JSONFileDestination(FileDestination):
    """JSON file destination for writing data as JSON lines asynchronously."""

    def __init__(self, file_path: str, monitor: PipelineMonitor, encoding: str = "utf-8"):
        """
        Initialize the JSON file destination.

        Args:
            file_path: Path to the output JSON file.
            monitor: PipelineMonitor instance for logging and metrics.
            encoding: File encoding (default: utf-8).
        """
        self.file_path = file_path
        self.monitor = monitor
        self.encoding = encoding

    async def send(self, data: Any) -> None:
        """Write data as individual JSON objects to file.

        Args:
            data: Data to write (expected to be serializable to JSON, e.g., dict).
        """
        try:
            json_data = json.dumps(data, ensure_ascii=False)
            async with aiofiles.open(self.file_path, mode="a", encoding=self.encoding) as f:
                await f.write(json_data + "\n")  # Write as JSON Lines format
        except Exception as e:
            self.monitor.log_error(f"Error writing JSON to {self.file_path}: {e}")
            raise
