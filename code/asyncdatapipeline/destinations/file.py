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
        self._header_written = False
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self._file_path), exist_ok=True)

    async def send(self, data: dict) -> None:
        """Write data to file asynchronously."""
        try:
            async with aiofiles.open(self._file_path, mode="a", encoding=self._encoding) as f:
                if not self._header_written:
                    # Write headers only once
                    await f.write(",".join(data.keys()) + "\n")
                    self._header_written = True
                await f.write(",".join(map(str, data.values())) + "\n")
            self.monitor.log_debug(f"Wrote data to {self._file_path}")
        except Exception as e:
            self.monitor.log_error(f"Error writing to file {self._file_path}: {e}")
            raise


class CSVFileDestination(FileDestination):
    """CSV file destination for writing data to CSV files."""

    def __init__(self, file_path: str, monitor: PipelineMonitor, encoding: str = "utf-8"):
        super().__init__(file_path, monitor, encoding)
        self._header_written = False

    async def send(self, data: dict) -> None:
        """Write data to CSV file asynchronously."""
        try:
            async with aiofiles.open(self._file_path, mode="a", encoding=self._encoding) as f:
                writer = AsyncWriter(f)
                if not self._header_written:
                    # Write headers only once
                    await writer.writerow(data.keys())
                    self._header_written = True
                await writer.writerow(data.values())
            self.monitor.log_debug(f"Wrote data {data.values()} to {self._file_path}")
        except Exception as e:
            self.monitor.log_error(f"Error writing to file {self._file_path}: {e}")
            raise


class JSONFileDestination(FileDestination):
    """JSON file destination for writing data as a single JSON array asynchronously."""

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
        self._initialized = False

    async def send(self, data: Any) -> None:
        """Write data to JSON file as part of a single array asynchronously.

        Args:
            data: Data to write (expected to be serializable to JSON, e.g., dict).
        """
        try:
            json_data = json.dumps(data, ensure_ascii=False)
            async with aiofiles.open(self.file_path, mode="a", encoding=self.encoding) as f:
                if not self._initialized:
                    # Start JSON array
                    await f.write("[")
                    self._initialized = True
                else:
                    # Add comma separator for subsequent items
                    await f.write(",")
                await f.write(json_data)
            self.monitor.log_event(f"Wrote JSON data to {self.file_path}")
        except Exception as e:
            self.monitor.log_error(f"Error writing JSON to {self.file_path}: {e}")
            raise
