"""Base file source implementation for async data pipeline."""

from typing import AsyncGenerator, List, Optional, Any
import os
import asyncio

import aiofiles

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.base import Source


class FileSource(Source):
    """File source reading large files line-by-line."""

    def __init__(
        self,
        file_path: str,
        monitor: PipelineMonitor,
        encoding: str = "utf-8",
        multipart_enabled: bool = True,
        chunk_size: int = 1024 * 1024  # Default 1MB chunks
    ):
        super().__init__(monitor)
        self._file_path = file_path
        self._encoding = encoding
        self._multipart_enabled = multipart_enabled
        self._chunk_size = chunk_size
        self._file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

    async def _read_standard(self) -> AsyncGenerator[str, None]:
        """Standard line-by-line reading from file."""
        async with aiofiles.open(self._file_path, mode="r", encoding=self._encoding) as file:
            async for line in file:
                self.monitor.log_debug(f"Read line from {self._file_path}")
                yield line.strip()

    async def _read_multipart(self) -> AsyncGenerator[str, None]:
        """Read file in chunks for efficient processing of large files."""
        # Determine number of chunks
        num_chunks = max(1, (self._file_size + self._chunk_size - 1) // self._chunk_size)
        incomplete_lines = []

        for chunk_idx in range(num_chunks):
            start_pos = chunk_idx * self._chunk_size
            end_pos = min((chunk_idx + 1) * self._chunk_size, self._file_size)

            # Read chunk
            async with aiofiles.open(self._file_path, mode="rb") as file:
                await file.seek(start_pos)
                chunk_data = await file.read(end_pos - start_pos)

            # Decode chunk, handling encoding issues
            try:
                chunk_text = chunk_data.decode(self._encoding)
            except UnicodeDecodeError:
                # Handle potential split multi-byte characters at chunk boundaries
                self.monitor.log_warning(f"Decode error in chunk {chunk_idx}, adjusting boundary")
                # Try reading a few more bytes to complete character
                async with aiofiles.open(self._file_path, mode="rb") as file:
                    await file.seek(start_pos)
                    chunk_data = await file.read(end_pos - start_pos + 4)  # Read a few extra bytes
                chunk_text = chunk_data.decode(self._encoding, errors='replace')

            # Process lines in the chunk
            lines = chunk_text.splitlines()

            # If we have an incomplete line from previous chunk, prepend it
            if incomplete_lines and chunk_idx > 0:
                lines[0] = incomplete_lines.pop() + lines[0]

            # All complete lines except the last one can be yielded
            for line in lines[:-1]:
                self.monitor.log_debug(f"Read line from chunk {chunk_idx} of {self._file_path}")
                yield line

            # Save the last line as it might be incomplete (unless last chunk)
            if chunk_idx < num_chunks - 1:
                incomplete_lines = [lines[-1]]
            else:
                # Last chunk, yield the final line too
                yield lines[-1]

    async def generate(self) -> AsyncGenerator[str, None]:
        """Generate data from file source."""
        try:
            # Determine if multipart reading would be beneficial
            should_use_multipart = (
                self._multipart_enabled and
                self._file_size > self._chunk_size and
                os.path.exists(self._file_path)
            )
            self.monitor.log_event(f"File size: {self._file_size} bytes, Chunk size: {self._chunk_size} bytes")
            self.monitor.log_event(f"Using multipart: {should_use_multipart}")

            if should_use_multipart:
                self.monitor.log_debug(f"Using multipart processing for {self._file_path}")
                async for line in self._read_multipart():
                    yield line
            else:
                async for line in self._read_standard():
                    yield line
        except Exception as e:
            self.monitor.log_error(f"Error reading file {self._file_path}: {e}")
            raise
