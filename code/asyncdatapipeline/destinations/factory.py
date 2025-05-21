"""Factory functions for creating data destinations."""

import asyncio
import os
from typing import Any, Callable, Dict, Coroutine, Optional

from asyncdatapipeline.destinations.api import ApiDestination
from asyncdatapipeline.destinations.file import (CSVFileDestination,
                                                 FileDestination,
                                                 JSONFileDestination)
from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.destinations.sql_db import PostgreSQLDestination
from asyncdatapipeline.destinations.no_sql_db import MongoDBDestination


def file_destination(
    monitor: PipelineMonitor,
    file_path: str = "outputs/output.txt",
    **kwargs
) -> Callable[[Any], Coroutine[Any, Any, None]]:
    """Factory function to create a FileDestination instance."""
    # Create directory if it doesn't exist
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        monitor.log_event(f"Created directory: {directory}")

    destination_dict = {
        ".csv": CSVFileDestination,
        ".json": JSONFileDestination,
        ".txt": FileDestination,
    }
    _, file_extension = os.path.splitext(file_path)
    destination_constructor = destination_dict.get(file_extension, FileDestination)
    destination = destination_constructor(file_path, monitor, **kwargs)

    async def wrapper(data: Any) -> None:
        await destination(data)

    return wrapper


def sql_destination(
    monitor: PipelineMonitor,
    db_config: Dict[str, Any],
    **kwargs
) -> Callable[[Any], Coroutine[Any, Any, None]]:
    """Factory function to create a PostgreSQLDestination instance."""
    destination_dict = {
        "postgresql": PostgreSQLDestination,
        # Add other DB types here if needed
    }
    destination_constructor = destination_dict.get(db_config['name'], PostgreSQLDestination)
    destination = destination_constructor(
        db_config["credentials"], monitor, db_config["table_name"], db_config["columns"], **kwargs)

    async def wrapper(data: Any) -> None:
        await destination(data)

    return wrapper


def no_sql_destination(
    monitor: PipelineMonitor,
    db_config: Dict[str, Any],
    **kwargs
) -> Callable[[Any], Coroutine[Any, Any, None]]:
    """Factory function to create a MongoDBDestination instance."""
    destination_dict = {
        "mongodb": MongoDBDestination,
        # Add other DB types here if needed
    }
    destination_constructor = destination_dict.get(db_config['name'], MongoDBDestination)
    destination = destination_constructor(db_config, monitor, **kwargs)

    async def wrapper(data: Any) -> None:
        await destination(data)

    return wrapper
