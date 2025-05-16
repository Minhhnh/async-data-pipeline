import aiofiles
import aiohttp
from typing import Any
from functools import wraps
from asyncdatapipeline.monitoring import PipelineMonitor
import asyncio
from asyncdatapipeline.config import PipelineConfig


def retry_decorator(config: PipelineConfig):
    """Decorator for retrying destination operations."""

    def decorator(func):
        @wraps(func)
        async def wrapper(data: Any, monitor: PipelineMonitor):
            for attempt in range(config.retry_attempts):
                try:
                    await func(data, monitor)
                    monitor.log_debug(f"Sent data to {func.__name__}")
                    return
                except Exception as e:
                    monitor.log_error(f"Error in {func.__name__}, attempt {attempt + 1}/{config.retry_attempts}: {e}")
                    if attempt < config.retry_attempts - 1:
                        await asyncio.sleep(config.retry_delay)
                    else:
                        monitor.log_error(f"Failed after {config.retry_attempts} attempts in {func.__name__}")

        return wrapper

    return decorator


@retry_decorator(PipelineConfig())
async def file_destination(data: Any, monitor: PipelineMonitor, file_path: str = "outputs/output.txt") -> None:
    """Asynchronous file destination for writing data."""
    async with aiofiles.open(file_path, mode="a") as f:
        await f.write(str(data) + "\n")


@retry_decorator(PipelineConfig())
async def api_destination(data: Any, monitor: PipelineMonitor) -> None:
    """Asynchronous API destination for sending data."""
    async with aiohttp.ClientSession() as session:
        async with session.post("https://example.com/api", json={"data": str(data)}) as resp:
            if resp.status != 200:
                raise ValueError(f"API error: {resp.status}")
