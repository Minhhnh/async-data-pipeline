"""Twitter data source implementation."""

import asyncio
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Dict

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.base import Source
from dateutil.parser import parse
from twikit import Client, NotFound, TooManyRequests, Unauthorized
import os

from asyncdatapipeline.config import PipelineConfig


class TwitterSource(Source):
    """Twitter/X stream source using twikit."""

    def __init__(self, config: PipelineConfig, monitor: PipelineMonitor, query: str = "#tech") -> None:
        super().__init__(monitor)
        self._config: PipelineConfig = config
        self.query: str = query
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(self._config.max_concurrent_tasks)
        self.cookie_path: str = self._config.cookie_path
        self.credentials: Dict[str, str] = self._config.twitter_credentials

    async def initialize_client(self) -> Client:
        """Initialize and authenticate Twitter client."""
        client: Client = Client("en-US")
        cookie_auth_successful: bool = False
        try:
            client.load_cookies(self.cookie_path)
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
                    password=self.credentials["password"],
                )
                client.save_cookies(self.cookie_path)
                self.monitor.log_event("Successfully authenticated and saved new cookies")
            except Exception as e:
                self.monitor.log_error(f"Failed to authenticate with credentials: {e}")
                raise
        return client

    async def generate(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate tweets from Twitter/X."""
        while True:
            try:
                client: Client = await self.initialize_client()
                async with self.semaphore:
                    tweets = await client.search_tweet(self.query, product="Latest")
                for tweet in tweets:
                    tweet_time: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    yield {
                        "timestamp": tweet_time,
                        "username": tweet.user.name,
                        "text": tweet.text.replace(",", " "),
                        "created_at": parse(tweet.created_at).strftime("%Y-%m-%d %H:%M:%S"),
                        "retweets": tweet.retweet_count,
                        "likes": tweet.favorite_count,
                    }
            except TooManyRequests as too_many_requests:
                self.monitor.log_error("Rate limit reached. Waiting for reset")
                reset_timestamp: float = (
                    too_many_requests.rate_limit_reset
                    or datetime.now().timestamp() + timedelta(minutes=5).total_seconds()
                )
                reset_time: datetime = datetime.fromtimestamp(reset_timestamp)
                self.monitor.log_event(f"Rate limit reached. Waiting until {reset_time}")
                wait_time: float = (reset_time - datetime.now()).total_seconds() + 5
                await asyncio.sleep(max(wait_time, 0))
                continue
            except NotFound as not_found:
                self.monitor.log_error(f"Tweet not found: {not_found}")
                continue
            except Unauthorized:
                self.monitor.log_error("Authentication error detected. Attempting to re-authenticate")
                try:
                    # Remove expired cookies
                    if os.path.exists("cookies.json"):
                        os.remove("cookies.json")
                        self.monitor.log_event("Removed expired cookies")
                except Exception as cookie_error:
                    self.monitor.log_error(f"Failed to remove cookie file: {cookie_error}")

                # Wait before retry
                await asyncio.sleep(5)
                continue
            except Exception as e:
                self.monitor.log_error(f"Error fetching tweets: {e}")
                await asyncio.sleep(5)
                continue
