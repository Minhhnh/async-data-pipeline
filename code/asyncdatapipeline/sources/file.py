"""File data source implementation for async data pipeline."""

from typing import AsyncGenerator, List

import aiofiles
from aiocsv import AsyncReader

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.base import Source


class FileSource(Source):
    """File source reading large files line-by-line."""

    def __init__(self, file_path: str, monitor: PipelineMonitor, encoding: str = "utf-8"):
        super().__init__(monitor)
        self._file_path = file_path
        self._encoding = encoding

    async def generate(self) -> AsyncGenerator[str, None]:
        """Generate data from file source."""
        try:
            async with aiofiles.open(self._file_path, mode="r", encoding=self._encoding) as file:
                async for line in file:
                    self.monitor.log_debug(f"Read line from {self._file_path}")
                    yield line.strip()
        except Exception as e:
            self.monitor.log_error(f"Error reading file {self._file_path}: {e}")
            raise


class CSVFileSource(FileSource):
    """CSV file source reading large CSV files line-by-line.

    Args:
        file_path (str): Path to the CSV file.
        monitor (PipelineMonitor): Monitor instance for logging.
        encoding (str): File encoding. Defaults to "utf-8".
        delimiter (str): CSV delimiter. Defaults to ",".
        has_header (bool): Whether the CSV file has a header row. Defaults to True.
    """

    def __init__(self, file_path: str, monitor: PipelineMonitor, encoding: str = "utf-8",
                 delimiter: str = ",", has_header: bool = True):
        super().__init__(file_path, monitor, encoding)
        self.delimiter = delimiter
        self.has_header = has_header

    async def generate(self) -> AsyncGenerator[List[str], None]:
        """Generate data from CSV file source."""
        try:
            async with aiofiles.open(self._file_path, mode="r", encoding=self._encoding) as file:
                reader = AsyncReader(file, delimiter=self.delimiter)

                # Skip header row if has_header is True
                if self.has_header:
                    await reader.__anext__()
                    self.monitor.log_debug(f"Skipped header row from {self._file_path}")

                async for row in reader:
                    self.monitor.log_debug(f"Read row from {self._file_path}")
                    yield row
        except Exception as e:
            self.monitor.log_error(f"Error reading CSV file {self._file_path}: {e}")
            raise
