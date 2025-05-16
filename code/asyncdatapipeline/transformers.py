from typing import Any
from .monitoring import PipelineMonitor

def uppercase_transformer(data: str, monitor: PipelineMonitor) -> str:
    """Transforms string data to uppercase."""
    result = data.upper()
    monitor.log_event(f"Transformed data to uppercase: {result}")
    return result

def keyword_filter_transformer(data: str, keyword: str, monitor: PipelineMonitor) -> str:
    """Filters data containing a specific keyword."""
    if keyword in data:
        monitor.log_event(f"Filtered data with keyword {keyword}")
        return data
    return None