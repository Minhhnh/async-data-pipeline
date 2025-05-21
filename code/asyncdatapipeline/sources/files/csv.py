"""CSV file source implementation for async data pipeline."""

from typing import AsyncGenerator, List
import os

import aiofiles
from aiocsv import AsyncReader

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.files.base import FileSource


class CSVFileSource(FileSource):
    """CSV file source reading large CSV files line-by-line.

    Args:
        file_path (str): Path to the CSV file.
        monitor (PipelineMonitor): Monitor instance for logging.
        encoding (str): File encoding. Defaults to "utf-8".
        delimiter (str): CSV delimiter. Defaults to ",".
        has_header (bool): Whether the CSV file has a header row. Defaults to True.
        multipart_enabled (bool): Whether to use multipart processing for large files.
        chunk_size (int): Size of chunks in bytes for multipart processing.
    """

    def __init__(
        self,
        file_path: str,
        monitor: PipelineMonitor,
        encoding: str = "utf-8",
        delimiter: str = ",",
        has_header: bool = True,
        multipart_enabled: bool = True,
        chunk_size: int = 1024 * 1024  # Default 1MB chunks
    ):
        super().__init__(file_path, monitor, encoding, multipart_enabled, chunk_size)
        self.delimiter = delimiter
        self.has_header = has_header
        self._header = None

    async def _read_csv_standard(self) -> AsyncGenerator[List[str], None]:
        """Standard CSV reading approach."""
        async with aiofiles.open(self._file_path, mode="r", encoding=self._encoding) as file:
            reader = AsyncReader(file, delimiter=self.delimiter)

            # Handle header row
            if self.has_header:
                self._header = await reader.__anext__()
                self.monitor.log_debug(f"Skipped header row from {self._file_path}")

            async for row in reader:
                self.monitor.log_debug(f"Read row from {self._file_path}")
                yield row

    async def _read_csv_multipart(self) -> AsyncGenerator[List[str], None]:
        """Multipart CSV reading using line-based approach."""
        # First, get the header if needed
        if self.has_header:
            async with aiofiles.open(self._file_path, mode="r", encoding=self._encoding) as file:
                first_line = await file.readline()
                self._header = first_line.strip().split(self.delimiter)
                self.monitor.log_debug(f"Read header from {self._file_path}: {self._header}")

        # Read the file line by line using multipart for efficiency
        first_row = True
        async for line in self._read_multipart():
            # Skip the first real line if we have a header
            if first_row and self.has_header:
                first_row = False
                continue

            # Parse and yield the CSV row
            row = line.split(self.delimiter)
            self.monitor.log_debug(f"Read CSV row from {self._file_path}")
            yield row

    async def generate(self) -> AsyncGenerator[List[str], None]:
        """Generate data from CSV file source."""
        try:
            # Determine if multipart reading would be beneficial
            should_use_multipart = (
                self._multipart_enabled and
                self._file_size > self._chunk_size and
                os.path.exists(self._file_path)
            )

            if should_use_multipart:
                self.monitor.log_debug(f"Using multipart processing for CSV {self._file_path}")
                async for row in self._read_csv_multipart():
                    yield row
            else:
                async for row in self._read_csv_standard():
                    yield row
        except Exception as e:
            self.monitor.log_error(f"Error reading CSV file {self._file_path}: {e}")
            raise
