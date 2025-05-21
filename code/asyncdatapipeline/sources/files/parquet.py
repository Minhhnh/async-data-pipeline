"""Parquet file source implementation for async data pipeline."""

from typing import AsyncGenerator, Dict, Any, Optional, List
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pyarrow.parquet as pq

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.file import FileSource


class ParquetFileSource(FileSource):
    """Parquet file source for reading Parquet data files.

    Args:
        file_path (str): Path to the Parquet file.
        monitor (PipelineMonitor): Monitor instance for logging.
        batch_size (int): Number of rows to read at once. Defaults to 1000.
        columns (Optional[List[str]]): List of columns to read. If None, all columns are read.
        use_threads (bool): Whether to use multithreading for Parquet reading. Defaults to True.
        filters (Optional[List]): PyArrow filters to apply when reading. Defaults to None.
    """

    def __init__(
        self,
        file_path: str,
        monitor: PipelineMonitor,
        batch_size: int = 1000,
        columns: Optional[List[str]] = None,
        use_threads: bool = True,
        filters: Optional[List] = None,
    ):
        super().__init__(file_path, monitor)
        self._file_path = file_path
        self._batch_size = batch_size
        self._columns = columns
        self._use_threads = use_threads
        self._filters = filters
        self._executor = ThreadPoolExecutor(max_workers=2)  # Limit thread count

    async def _read_metadata(self) -> Dict[str, Any]:
        """Read Parquet file metadata asynchronously."""
        def _get_metadata():
            parquet_file = pq.ParquetFile(self._file_path)
            metadata = {
                "num_rows": parquet_file.metadata.num_rows,
                "num_columns": len(parquet_file.schema.names),
                "schema": parquet_file.schema,
                "columns": parquet_file.schema.names,
                "file_size_bytes": os.path.getsize(self._file_path)
            }
            return metadata

        return await asyncio.get_event_loop().run_in_executor(self._executor, _get_metadata)

    async def _read_batch(self, row_offset: int, batch_size: int) -> List[Dict[str, Any]]:
        """Read a batch of rows from the Parquet file asynchronously."""
        def _read_batch_sync():
            table = pq.read_table(
                self._file_path,
                columns=self._columns,
                filters=self._filters,
                use_threads=self._use_threads
            )
            # Convert to pandas and then to dict records for easier processing
            return table.to_pandas().to_dict(orient="records")

        return await asyncio.get_event_loop().run_in_executor(self._executor, _read_batch_sync)

    async def generate(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate data from Parquet file source."""
        try:
            # Get metadata first to know total rows
            metadata = await self._read_metadata()
            total_rows = metadata["num_rows"]
            self.monitor.log_event(f"Parquet file {self._file_path} has {total_rows} rows")

            # Read in batches
            for offset in range(0, total_rows, self._batch_size):
                batch_size = min(self._batch_size, total_rows - offset)
                rows = await self._read_batch(offset, batch_size)

                for row in rows:
                    self.monitor.log_debug(f"Read row {offset} from Parquet file {self._file_path}")
                    yield row

        except Exception as e:
            self.monitor.log_error(f"Error reading Parquet file {self._file_path}: {e}")
            raise
        finally:
            self._executor.shutdown(wait=False)
