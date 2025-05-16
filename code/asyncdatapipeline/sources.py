from abc import ABC, abstractmethod
import asyncio
import csv
import os
import time
from datetime import datetime
from typing import Any, AsyncGenerator, Dict

import aiofiles
import aiohttp
from twikit import Client, TooManyRequests, NotFound

from asyncdatapipeline.config import PipelineConfig
from asyncdatapipeline.monitoring import PipelineMonitor


class Source(ABC):
    """Abstract base class for all data sources."""

    def __init__(self, monitor: PipelineMonitor):
        """
        Initialize the source with a monitor for logging and metrics.

        Args:
            monitor: PipelineMonitor instance for tracking events and errors.
        """
        self.monitor = monitor

    @abstractmethod
    async def generate(self) -> AsyncGenerator[Any, None]:
        """Generate data from the source asynchronously."""
        pass

    async def __call__(self) -> AsyncGenerator[Any, None]:
        """Make the source callable for pipeline compatibility."""
        try:
            async for item in self.generate():
                yield item
        except Exception as e:
            self.monitor.log_error(f"Error in source {self.__class__.__name__}: {e}")
            raise

    def __aiter__(self):
        """Make the source async iterable, returning an iterator for __call__."""
        return self.__call__().__aiter__()


class TwitterSource(Source):
    """Twitter/X stream source using twikit."""

    def __init__(self, credentials: Dict[str, str], monitor: PipelineMonitor, query: str = "#tech"):
        super().__init__(monitor)
        self.credentials = credentials
        self.query = query

    async def initialize_client(self) -> Client:
        """Initialize and authenticate Twitter client."""
        client = Client("en-US")
        cookie_auth_successful = False
        try:
            client.load_cookies("cookies.json")
            self.monitor.log_event("Using saved cookies for authentication")
            cookie_auth_successful = True
        except Exception as cookie_error:
            self.monitor.log_event(f"Cookie loading failed: {cookie_error}")

        if not cookie_auth_successful:
            self.monitor.log_event("Logging in with credentials")
            try:
                await client.login(
                    auth_info_1=self.credentials["username"],
                    auth_info_2=self.credentials["email"],
                    password=self.credentials["password"]
                )
                client.save_cookies("cookies.json")
                self.monitor.log_event("Successfully authenticated and saved new cookies")
            except Exception as e:
                self.monitor.log_error(f"Failed to authenticate with credentials: {e}")
                raise
        return client

    async def generate(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate tweets from Twitter/X."""
        while True:
            try:
                client = await self.initialize_client()
                tweets = await client.search_tweet(self.query, product="Latest")
                for tweet in tweets:
                    tweet_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    yield {
                        "timestamp": tweet_time,
                        "username": tweet.user.name,
                        "text": tweet.text.replace(",", " "),
                        "created_at": tweet.created_at,
                        "retweets": tweet.retweet_count,
                        "likes": tweet.favorite_count,
                    }
            except TooManyRequests as e:
                reset_time = datetime.fromtimestamp(e.rate_limit_reset)
                self.monitor.log_event(f"Rate limit reached. Waiting until {reset_time}")
                wait_time = (reset_time - datetime.now()).total_seconds() + 5
                await asyncio.sleep(max(wait_time, 0))
                continue
            except NotFound as e:
                self.monitor.log_event(f"Tweet not found: {e}")
                continue
            except Exception as e:
                self.monitor.log_error(f"Error fetching tweets: {e}")
                await asyncio.sleep(5)
                continue


class FileSource(Source):
    """File source reading large files line-by-line."""

    def __init__(self, file_path: str, monitor: PipelineMonitor, encoding: str = "utf-8"):
        super().__init__(monitor)
        self.file_path = file_path
        self.encoding = encoding

    async def generate(self) -> AsyncGenerator[str, None]:
        """Generate data from file source."""
        try:
            async with aiofiles.open(self.file_path, mode="r", encoding=self.encoding) as f:
                async for line in f:
                    self.monitor.log_debug(f"Read line from {self.file_path}")
                    yield line.strip()
        except Exception as e:
            self.monitor.log_error(f"Error reading file {self.file_path}: {e}")
            raise


class ApiSource(Source):
    """API source fetching JSON data."""

    def __init__(self, url: str, monitor: PipelineMonitor):
        super().__init__(monitor)
        self.url = url

    async def generate(self) -> AsyncGenerator[dict, None]:
        """Generate data from API source."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in data:
                            self.monitor.log_event(f"Fetched item from {self.url}")
                            yield item
                    else:
                        raise ValueError(f"API error: {resp.status}")
        except Exception as e:
            self.monitor.log_error(f"Error fetching API {self.url}: {e}")
            raise

# Factory Functions


def twitter_source(credentials: dict, monitor: PipelineMonitor, query: str = "#tech") -> TwitterSource:
    """Factory function to create a TwitterSource instance."""
    return TwitterSource(credentials, monitor, query)


def file_source(file_path: str, monitor: PipelineMonitor, **kwargs) -> FileSource:
    """Factory function to create a FileSource instance."""
    return FileSource(file_path, monitor, **kwargs)


def api_source(url: str, monitor: PipelineMonitor) -> ApiSource:
    """Factory function to create an ApiSource instance."""
    return ApiSource(url, monitor)
