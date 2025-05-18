"""Twitter data source implementation."""

import asyncio
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Dict

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.base import Source
from dateutil.parser import parse
from twikit import Client, NotFound, TooManyRequests


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
                    password=self.credentials["password"],
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
                        "created_at": parse(tweet.created_at).strftime("%Y-%m-%d %H:%M:%S"),
                        "retweets": tweet.retweet_count,
                        "likes": tweet.favorite_count,
                    }
            except TooManyRequests as too_many_requests:
                reset_timestamp = (
                    too_many_requests.rate_limit_reset
                    or datetime.now().timestamp() + timedelta(minutes=5).total_seconds()
                )
                reset_time = datetime.fromtimestamp(reset_timestamp)
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
