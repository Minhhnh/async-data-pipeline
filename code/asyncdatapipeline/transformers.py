from typing import Any
from asyncdatapipeline.monitoring import PipelineMonitor


def uppercase_transformer(data: str, monitor: PipelineMonitor) -> str:
    """Transforms string data to uppercase."""
    if not isinstance(data, str):
        return data
    result = data.upper()
    return result


def keyword_filter_transformer(data: str, keyword: str, monitor: PipelineMonitor) -> str:
    """Filters data containing a specific keyword."""
    if keyword in data:
        monitor.log_debug(f"Filtered data with keyword {keyword}")
        return data
    return ""


def transform_csv_line_to_dict(line: Any, monitor: PipelineMonitor) -> dict:
    """Transforms a CSV line to a dict object."""
    if isinstance(line, dict):
        monitor.log_debug(f"Line is already a dict: {line}")
        return line
    try:
        monitor.log_event(f"Transforming line to dict: {line}")
        parts = line
        if isinstance(line, str):
            parts = line.strip().split(',')
        if len(parts) < 5:
            monitor.log_error(f"Invalid line format: {line}")
            return {}
        tweet_data = {
            "timestamp": parts[0],
            "username": parts[1],
            "text": parts[2],
            "created_at": parts[3],
            "retweets": parts[4],
            "likes": int(parts[5]),
        }
        return tweet_data
    except ValueError as e:
        monitor.log_error(f"Error transforming line to dict: {line} - {e}")
        return {}
