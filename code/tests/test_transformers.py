import asyncio
import pytest
from unittest.mock import MagicMock, patch
from typing import Dict, Any

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.transformers.base import BaseTransformer
from asyncdatapipeline.transformers import (
    uppercase_transformer,
    deduplication_transformer,
    csv_dict_transformer,
)


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


@pytest.mark.asyncio
async def test_uppercase_transformer(monitor):
    """Test the uppercase transformer."""
    transformer = uppercase_transformer(monitor)

    # Test with a string
    result = await transformer("hello world")
    assert result == "HELLO WORLD"

    # Test with a dictionary with text key
    data = {"text": "hello world", "username": "testuser"}
    result = await transformer(data)
    assert result["text"] == "hello world"
    assert result["username"] == "testuser"  # Should not change


@pytest.mark.asyncio
async def test_deduplication_transformer(monitor):
    """Test the deduplication transformer."""
    transformer = deduplication_transformer(monitor, capacity=100, error_rate=0.01)

    # First item should pass through
    data1 = {"id": "123", "text": "Hello world"}
    result1 = await transformer(data1)
    assert result1 == data1

    # Duplicate item should be filtered (return None)
    result2 = await transformer(data1)
    assert result2 is None

    # Different item should pass through
    data3 = {"id": "456", "text": "Different item"}
    result3 = await transformer(data3)
    assert result3 == data3


@pytest.mark.asyncio
async def test_transformer_error_handling(monitor):
    """Test error handling in transformers."""
    class ErrorTransformer(BaseTransformer):
        async def transform(self, data):
            raise ValueError("Test error")

    transformer = ErrorTransformer(monitor)

    # Error should be logged and re-raised
    with pytest.raises(ValueError):
        await transformer("test data")

    # Check that error was logged
    monitor.log_error.assert_called_once()


@pytest.mark.asyncio
@patch("asyncdatapipeline.transformers.nsfw.AutoTokenizer")
@patch("asyncdatapipeline.transformers.nsfw.AutoModelForSequenceClassification")
async def test_nsfw_transformer_mocked(mock_model_class, mock_tokenizer_class, monitor):
    """Test the NSFW transformer with mocked HuggingFace components."""
    from asyncdatapipeline.transformers import nsfw_transformer

    # Setup mocks
    mock_tokenizer = MagicMock()
    mock_tokenizer.return_value = {"input_ids": MagicMock(), "attention_mask": MagicMock()}
    mock_tokenizer_class.from_pretrained.return_value = mock_tokenizer

    mock_model = MagicMock()
    mock_outputs = MagicMock()
    mock_outputs.logits = MagicMock()
    mock_model.return_value = mock_outputs
    mock_model_class.from_pretrained.return_value = mock_model

    # Create transformer
    transformer = nsfw_transformer(monitor, threshold=0.7)

    # Test with safe content (mocked)
    with patch.object(transformer, "detect_nsfw", return_value={"safe": 0.9, "nsfw": 0.1}):
        data = {"text": "This is safe content"}
        result = await transformer(data)
        assert result == data

    # Test with unsafe content (mocked)
    with patch.object(transformer, "detect_nsfw", return_value={"safe": 0.2, "nsfw": 0.8}):
        data = {"text": "This is unsafe content"}
        result = await transformer(data)
        assert result is None
