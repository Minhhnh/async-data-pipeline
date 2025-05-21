"""Configure pytest for asyncdatapipeline tests."""

from asyncdatapipeline.config import PipelineConfig
from asyncdatapipeline.monitoring import PipelineMonitor
import os
import sys
import pytest
import asyncio
from unittest.mock import MagicMock

# Add the code directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
code_dir = os.path.dirname(current_dir)
if code_dir not in sys.path:
    sys.path.insert(0, code_dir)


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for each test."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
def monitor():
    """Create a PipelineMonitor with mocked logging for testing."""
    monitor = PipelineMonitor()
    # Override the logger to avoid file writes during tests
    monitor.log_event = MagicMock()
    monitor.log_error = MagicMock()
    monitor.log_debug = MagicMock()
    monitor.log_warning = MagicMock()
    return monitor


@pytest.fixture
def config():
    """Create a test configuration."""
    return PipelineConfig(
        max_concurrent_tasks=2,
        retry_attempts=1,
        checkpoint_path=":memory:",  # No actual file will be written
        enable_recovery=False,
        multipart_enabled=True,
        multipart_chunk_size=1024,  # Small chunks for testing
    )
