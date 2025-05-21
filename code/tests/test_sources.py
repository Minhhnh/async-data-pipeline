import asyncio
import os
import tempfile
import pytest
from unittest.mock import MagicMock, patch
import aiofiles
import json

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.files.base import FileSource
from asyncdatapipeline.sources.files.csv import CSVFileSource
from asyncdatapipeline.sources.api import ApiSource


@pytest.fixture
def monitor():
    """Create a monitor fixture for testing."""
    monitor = PipelineMonitor()
    # Override the logger to avoid file writes during tests
    monitor.log_event = MagicMock()
    monitor.log_error = MagicMock()
    monitor.log_debug = MagicMock()
    monitor.log_warning = MagicMock()
    return monitor


@pytest.fixture
async def temp_text_file():
    """Create a temporary test file with sample content."""
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
        f.write("line 1\nline 2\nline 3\n")
        file_path = f.name

    yield file_path

    # Clean up
    os.unlink(file_path)


@pytest.fixture
async def temp_csv_file():
    """Create a temporary CSV file with sample content."""
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
        f.write("id,name,value\n")
        f.write("1,item1,100\n")
        f.write("2,item2,200\n")
        f.write("3,item3,300\n")
        file_path = f.name

    yield file_path

    # Clean up
    os.unlink(file_path)


@pytest.mark.asyncio
async def test_file_source_standard(monitor, temp_text_file):
    """Test the standard file source."""
    source = FileSource(temp_text_file, monitor)

    lines = []
    async for line in source.generate():
        lines.append(line)

    assert len(lines) == 3
    assert lines[0] == "line 1"
    assert lines[1] == "line 2"
    assert lines[2] == "line 3"


@pytest.mark.asyncio
async def test_csv_file_source(monitor, temp_csv_file):
    """Test the CSV file source."""
    source = CSVFileSource(temp_csv_file, monitor, has_header=True)

    rows = []
    async for row in source.generate():
        rows.append(row)

    assert len(rows) == 3
    assert rows[0] == ["1", "item1", "100"]
    assert rows[1] == ["2", "item2", "200"]
    assert rows[2] == ["3", "item3", "300"]


@pytest.mark.asyncio
async def test_multipart_file_reading(monitor, temp_text_file):
    """Test multipart file reading with small chunk size."""
    # Set a very small chunk size to force multipart reading
    source = FileSource(
        temp_text_file,
        monitor,
        multipart_enabled=True,
        chunk_size=10  # Very small chunk size
    )

    lines = []
    async for line in source.generate():
        lines.append(line)

    assert len(lines) == 3
    assert lines[0] == "line 1"
    assert lines[1] == "line 2"
    assert lines[2] == "line 3"


@pytest.mark.asyncio
async def test_api_source():
    """Test the API source with mocked HTTP responses."""
    monitor = MagicMock()

    # Mock the _fetch_from_rest method to return a sequence of items
    with patch("asyncdatapipeline.sources.api.ApiSource._fetch_from_rest") as mock_fetch:
        async def mock_generator():
            yield {"id": 1, "text": "test1"}
            yield {"id": 2, "text": "test2"}

        mock_fetch.return_value = mock_generator()

        source = ApiSource("http://test-api.com/data", monitor)

        # Collect items from the source
        items = []
        async for item in source.generate():
            items.append(item)
            if len(items) >= 2:  # Limit to avoid infinite loop in tests
                break

        assert len(items) == 2
        assert items[0]["id"] == 1
        assert items[1]["id"] == 2
