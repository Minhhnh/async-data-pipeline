"""Factory functions for creating data sources."""

from typing import Dict

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.twitter import TwitterSource
from asyncdatapipeline.sources.file import FileSource, CSVFileSource
from asyncdatapipeline.sources.api import ApiSource


def twitter_source(credentials: Dict, monitor: PipelineMonitor, query: str = "#tech") -> TwitterSource:
    """Factory function to create a TwitterSource instance."""
    return TwitterSource(credentials, monitor, query)


def file_source(file_path: str, monitor: PipelineMonitor, **kwargs) -> FileSource:
    """Factory function to create a FileSource instance."""
    if file_path.endswith(".csv"):
        return CSVFileSource(file_path, monitor, **kwargs)
    return FileSource(file_path, monitor, **kwargs)


def api_source(url: str, monitor: PipelineMonitor, **kwargs) -> ApiSource:
    """Factory function to create an ApiSource instance."""
    return ApiSource(url, monitor, locale=kwargs.get('locale', "en_US"), max_items=kwargs.get('max_items', 100))
