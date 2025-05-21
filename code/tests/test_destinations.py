import asyncio
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import aiofiles
import pytest
from asyncdatapipeline.destinations.api import ApiDestination
from asyncdatapipeline.destinations.file import (
    CSVFileDestination,
    FileDestination,
    JSONFileDestination,
)
from asyncdatapipeline.monitoring import PipelineMonitor


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
def temp_dir():
    """Create a temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.mark.asyncio
async def test_file_destination(monitor, temp_dir):
    """Test the file destination."""
    output_file = os.path.join(temp_dir, "output.txt")
    destination = FileDestination(output_file, monitor)

    test_data = {"id": 1, "text": "Hello world", "count": 42}
    await destination.send(test_data)

    # Verify the file was created and contains the expected content
    assert os.path.exists(output_file)

    async with aiofiles.open(output_file, "r") as f:
        content = await f.read()
        assert "1,Hello world,42" in content


@pytest.mark.asyncio
async def test_json_file_destination(monitor, temp_dir):
    """Test the JSON file destination."""
    output_file = os.path.join(temp_dir, "output.json")
    destination = JSONFileDestination(output_file, monitor)

    test_data = {"id": 1, "text": "Hello world", "count": 42}
    await destination.send(test_data)

    # Verify the file was created and contains the expected content
    assert os.path.exists(output_file)

    async with aiofiles.open(output_file, "r") as f:
        content = await f.read()
        loaded_data = json.loads(content)
        assert loaded_data["id"] == 1
        assert loaded_data["text"] == "Hello world"
        assert loaded_data["count"] == 42


@pytest.mark.asyncio
async def test_csv_file_destination(monitor, temp_dir):
    """Test the CSV file destination."""
    output_file = os.path.join(temp_dir, "output.csv")
    destination = CSVFileDestination(output_file, monitor)

    test_data = {"id": 1, "name": "test item", "value": 100}
    await destination.send(test_data)

    # Verify the file was created
    assert os.path.exists(output_file)

    # Read and verify content
    async with aiofiles.open(output_file, "r") as f:
        content = await f.read()
        assert "1,test item,100" in content


@pytest.mark.asyncio
async def test_api_destination():
    """Test the API destination with mocked HTTP responses."""
    monitor = MagicMock()

    # Mock ClientSession.post
    with patch("aiohttp.ClientSession.post") as mock_post:
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_post.return_value.__aenter__.return_value = mock_response

        destination = ApiDestination("http://test-api.com/data", monitor)

        # Send data
        test_data = {"id": 1, "text": "test data"}
        await destination.send(test_data)

        # Verify the API was called with the right data
        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        assert kwargs["json"]["data"] == test_data


@pytest.mark.asyncio
async def test_api_destination_error():
    """Test handling of API errors."""
    monitor = MagicMock()

    # Mock ClientSession.post with error response
    with patch("aiohttp.ClientSession.post") as mock_post:
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status = 500
        mock_post.return_value.__aenter__.return_value = mock_response

        destination = ApiDestination("http://test-api.com/data", monitor)

        # Send data and expect error
        test_data = {"id": 1, "text": "test data"}
        with pytest.raises(ValueError):
            await destination.send(test_data)

        # Verify the error was logged
        monitor.log_error.assert_called_once()
