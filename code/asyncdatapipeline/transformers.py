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


def transform_csv_line_to_dict(line: str, monitor: PipelineMonitor) -> dict:
    """Transforms a CSV line to a dict object."""
    if isinstance(line, dict):
        monitor.log_debug(f"Line is already a dict: {line}")
        return line
    try:
        parts = line.strip().split(',')
        print(parts)
        if len(parts) < 5:
            monitor.log_debug(f"Invalid line format: {line}")
            return {}
        tweet_data = {
            'Timestamp': parts[0],
            'Username': parts[1],
            'Text': parts[2],
            'Created At': parts[3],
            'Retweets': parts[4],
            'Likes': int(parts[5])
        }
        return tweet_data
    except ValueError as e:
        monitor.log_error(f"Error transforming line to dict: {line} - {e}")
        return {}
