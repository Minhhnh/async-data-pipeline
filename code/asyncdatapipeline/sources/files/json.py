"""JSON file source implementation for async data pipeline."""

from typing import AsyncGenerator, Dict, Any
import json

import aiofiles

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.files.base import FileSource


class JSONFileSource(FileSource):
    """JSON file source for reading JSON data from files.

    Supports both JSON Lines format (one JSON object per line) and 
    standard JSON format (single object or array).

    Args:
        file_path (str): Path to the JSON file.
        monitor (PipelineMonitor): Monitor instance for logging.
        encoding (str): File encoding. Defaults to "utf-8".
        lines_format (bool): Whether the file is in JSON Lines format. Defaults to True.
        multipart_enabled (bool): Whether to use multipart processing for large files.
        chunk_size (int): Size of chunks in bytes for multipart processing.
    """

    def __init__(
        self,
        file_path: str,
        monitor: PipelineMonitor,
        encoding: str = "utf-8",
        lines_format: bool = True,
        multipart_enabled: bool = True,
        chunk_size: int = 1024 * 1024  # Default 1MB chunks
    ):
        super().__init__(file_path, monitor, encoding, multipart_enabled, chunk_size)
        self.lines_format = lines_format

    async def _read_json_lines(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Read JSON Lines format - one JSON object per line."""
        async for line in self._read_standard() if not self._multipart_enabled else self._read_multipart():
            try:
                if line.strip():  # Skip empty lines
                    data = json.loads(line)
                    self.monitor.log_debug(f"Parsed JSON object from {self._file_path}")
                    yield data
            except json.JSONDecodeError as e:
                self.monitor.log_warning(f"Error parsing JSON line: {e}")

    async def _read_json_standard(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Read standard JSON format (entire file as one JSON object/array)."""
        try:
            # Read entire file at once - this is not chunked to preserve JSON validity
            async with aiofiles.open(self._file_path, mode="r", encoding=self._encoding) as file:
                content = await file.read()

            # Parse the JSON content
            data = json.loads(content)

            # If the data is an array, yield each item
            if isinstance(data, list):
                for item in data:
                    self.monitor.log_debug(f"Yielding item from JSON array in {self._file_path}")
                    yield item
            else:
                # If the data is a single object, yield it
                self.monitor.log_debug(f"Yielding single JSON object from {self._file_path}")
                yield data

        except json.JSONDecodeError as e:
            self.monitor.log_error(f"Error parsing JSON file {self._file_path}: {e}")
            raise

    async def generate(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate data from JSON file source."""
        try:
            if self.lines_format:
                # For JSON Lines format (newline-delimited JSON)
                async for data in self._read_json_lines():
                    yield data
            else:
                # For standard JSON format (entire file as one JSON object/array)
                async for data in self._read_json_standard():
                    yield data
        except Exception as e:
            self.monitor.log_error(f"Error reading JSON file {self._file_path}: {e}")
            raise
