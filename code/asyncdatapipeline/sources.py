import asyncio
import csv
import os
import time
from datetime import datetime
from typing import AsyncGenerator

import aiofiles
import aiohttp
from twikit import Client, TooManyRequests, NotFound

from asyncdatapipeline.config import PipelineConfig
from asyncdatapipeline.monitoring import PipelineMonitor


async def initialize_client(credentials):
    """Initialize and authenticate Twitter client"""
    client = Client("en-US")
    cookie_auth_successful = False

    monitor = PipelineMonitor()

    try:
        # First try with cookies
        client.load_cookies("cookies.json")
        monitor.log_event("Using saved cookies for authentication")
        cookie_auth_successful = True
    except Exception as cookie_error:
        # Log cookie loading error
        monitor.log_event(f"Cookie loading failed: {cookie_error}")

    # Only login with credentials if cookie authentication failed
    if not cookie_auth_successful:
        monitor.log_event("Logging in with credentials")
        await client.login(
            auth_info_1=credentials["username"], auth_info_2=credentials["email"], password=credentials["password"]
        )

        # Save the new cookies for future use
        client.save_cookies("cookies.json")
        monitor.log_event("Successfully authenticated and saved new cookies")
    return client


async def twitter_source(
    credentials: dict, monitor: PipelineMonitor, query: str = "#tech"
) -> AsyncGenerator[dict, None]:
    """Asynchronous Twitter/X stream source using twikit."""
    while True:
        # Initialize Twitter client
        client = await initialize_client(credentials)
        try:
            tweets = await client.search_tweet(query, product="Latest")
            for tweet in tweets:
                tweet_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield {
                    "Timestamp": tweet_time,
                    "Username": tweet.user.name,
                    "Text": tweet.text.replace(",", " "),
                    "Created At": tweet.created_at,
                    "Retweets": tweet.retweet_count,
                    "Likes": tweet.favorite_count,
                }
        except TooManyRequests as e:
            rate_limit_reset_time = datetime.fromtimestamp(e.rate_limit_reset)  # type: ignore
            monitor.log_event(f"Rate limit reached. Waiting until {rate_limit_reset_time}")
            # Calculate wait time
            wait_time = rate_limit_reset_time - datetime.now()
            await asyncio.sleep(wait_time.total_seconds() + 5)  # Add 5 seconds as buffer
            # Continue after waiting
            continue
        except NotFound as e:
            monitor.log_event(f"Tweet not found: {e}")
            # Handle NotFound error
            continue
        except Exception as e:
            monitor.log_event(f"Error fetching tweets: {e}")
            # Handle other exceptions
            await asyncio.sleep(5)


async def file_source(file_path: str, monitor: PipelineMonitor) -> AsyncGenerator[str, None]:
    """Asynchronous file source reading large files line-by-line."""
    try:
        async with aiofiles.open(file_path, mode="r") as f:
            async for line in f:
                monitor.log_event(f"Read line from {file_path}")
                yield line.strip()
    except Exception as e:
        monitor.log_error(f"Error reading file {file_path}: {e}")
        raise


async def api_source(url: str, monitor: PipelineMonitor) -> AsyncGenerator[dict, None]:
    """Asynchronous API source fetching JSON data."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data:
                        monitor.log_event(f"Fetched item from {url}")
                        yield item
                else:
                    raise ValueError(f"API error: {resp.status}")
    except Exception as e:
        monitor.log_error(f"Error fetching API {url}: {e}")
        raise
