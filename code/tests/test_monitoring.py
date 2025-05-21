import os
import pytest
import tempfile
import time
from unittest.mock import patch, MagicMock

from asyncdatapipeline.monitoring import PipelineMonitor, LoggingFormatter


def test_pipeline_monitor_initialization():
    """Test that PipelineMonitor initializes correctly."""
    with tempfile.NamedTemporaryFile() as tmp:
        with patch('logging.FileHandler') as mock_file_handler:
            monitor = PipelineMonitor()
            # Check that metrics are initialized correctly
            assert monitor.metrics["throughput"] == 0
            assert monitor.metrics["errors"] == 0
            assert isinstance(monitor.metrics["latency"], list)


def test_logging_methods():
    """Test the logging methods of PipelineMonitor."""
    monitor = PipelineMonitor()

    # Patch the logger to avoid actual logging
    monitor.logger = MagicMock()

    # Test each logging method
    monitor.log_debug("Debug message")
    monitor.logger.debug.assert_called_with("Debug message")

    monitor.log_event("Info message")
    monitor.logger.info.assert_called_with("Info message")

    monitor.log_warning("Warning message")
    monitor.logger.warning.assert_called_with("Warning message")

    monitor.log_error("Error message")
    monitor.logger.error.assert_called_with("Error message")

    # Error should increment the error count
    assert monitor.metrics["errors"] == 1


def test_track_processing():
    """Test the processing tracking functionality."""
    monitor = PipelineMonitor()

    # Simulate processing that takes 0.1 seconds
    start_time = time.time() - 0.1
    monitor.track_processing(start_time)

    # Check metrics
    assert monitor.metrics["throughput"] == 1
    assert len(monitor.metrics["latency"]) == 1
    assert 0.09 <= monitor.metrics["latency"][0] <= 0.2  # Allow for slight timing variations


def test_get_metrics():
    """Test the metrics calculation."""
    monitor = PipelineMonitor()

    # Add some test data
    monitor.metrics["throughput"] = 5
    monitor.metrics["latency"] = [0.1, 0.2, 0.3, 0.4, 0.5]
    monitor.metrics["errors"] = 2

    # Get metrics
    metrics = monitor.get_metrics()

    # Check calculated values
    assert metrics["throughput"] == 5
    assert metrics["avg_latency"] == 0.3  # Average of latency values
    assert metrics["errors"] == 2
