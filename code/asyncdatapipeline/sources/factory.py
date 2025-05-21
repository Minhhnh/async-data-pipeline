"""Factory functions for creating data sources."""

from typing import Dict, Optional, Any

from asyncdatapipeline.config import PipelineConfig
from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.api import ApiSource
from asyncdatapipeline.sources.files import FileSource, CSVFileSource, JSONFileSource, ParquetFileSource
from asyncdatapipeline.sources.twitter import TwitterSource
from asyncdatapipeline.sources.websocket import WebSocketSource


def twitter_source(config: PipelineConfig, monitor: PipelineMonitor, query: str = "#tech") -> TwitterSource:
    """Factory function to create a TwitterSource instance."""
    return TwitterSource(config, monitor, query)


def file_source(file_path: str, monitor: PipelineMonitor, **kwargs) -> Any:
    """Factory function to create a FileSource instance based on file extension."""
    # Make a copy of kwargs to avoid modifying the original
    kwargs_copy = kwargs.copy()

    # Common parameters that all file sources accept
    common_params = {
        "file_path": file_path,
        "monitor": monitor,
    }

    # Add encoding if provided (all file sources accept this)
    if "encoding" in kwargs_copy:
        common_params["encoding"] = kwargs_copy.pop("encoding")

    # Process by file type
    if file_path.endswith(".parquet"):
        # Parquet-specific parameters - only pass relevant parameters
        parquet_params = {**common_params}
        if "batch_size" in kwargs_copy:
            parquet_params["batch_size"] = kwargs_copy.pop("batch_size")
        if "columns" in kwargs_copy:
            parquet_params["columns"] = kwargs_copy.pop("columns")
        if "use_threads" in kwargs_copy:
            parquet_params["use_threads"] = kwargs_copy.pop("use_threads")
        if "filters" in kwargs_copy:
            parquet_params["filters"] = kwargs_copy.pop("filters")
        return ParquetFileSource(**parquet_params)

    # For all other file types, include multipart parameters
    if "multipart_enabled" in kwargs_copy:
        common_params["multipart_enabled"] = kwargs_copy.pop("multipart_enabled")
    if "chunk_size" in kwargs_copy:
        common_params["chunk_size"] = kwargs_copy.pop("chunk_size")

    if file_path.endswith(".csv"):
        # CSV-specific parameters
        csv_params = {**common_params}
        if "delimiter" in kwargs_copy:
            csv_params["delimiter"] = kwargs_copy.pop("delimiter")
        if "has_header" in kwargs_copy:
            csv_params["has_header"] = kwargs_copy.pop("has_header")
        return CSVFileSource(**csv_params)

    elif file_path.endswith((".json", ".jsonl")):
        # JSON-specific parameters
        json_params = {
            **common_params,
            "lines_format": file_path.endswith(".jsonl")
        }
        # Override lines_format if explicitly provided
        if "lines_format" in kwargs_copy:
            json_params["lines_format"] = kwargs_copy.pop("lines_format")
        return JSONFileSource(**json_params)

    # Default file source
    return FileSource(**common_params)


def api_source(url: str, monitor: PipelineMonitor, **kwargs) -> ApiSource:
    """Factory function to create an ApiSource instance."""
    return ApiSource(url, monitor, **kwargs)


def websocket_source(
    url: str,
    monitor: PipelineMonitor,
    **kwargs
) -> WebSocketSource:
    """Factory function to create a WebSocketSource instance."""
    return WebSocketSource(url, monitor, **kwargs)
