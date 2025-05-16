from typing import AsyncGenerator
import aiofiles
import aiohttp
import asyncio
from twikit import Client
from .monitoring import PipelineMonitor
from .config import PipelineConfig
import csv
import os
from datetime import datetime
import time
from twikit import TooManyRequests


async def twitter_source(credentials: dict, monitor: PipelineMonitor, query: str = "#tech") -> AsyncGenerator[str, None]:
    """Asynchronous Twitter/X stream source using twikit."""

    # Create CSV file with timestamp in filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"/Users/minhhnh/Minh.Ho/tweets_{timestamp}.csv"

    # Create CSV file and write header
    with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'Tweet'])

    monitor.log_event(f"Created CSV file: {csv_filename}")

    while True:
        try:
            client = Client('en-US')
            cookie_auth_successful = False

            try:
                # First try with cookies
                client.load_cookies('cookies.json')
                monitor.log_event("Using saved cookies for authentication")
                cookie_auth_successful = True
            except Exception as cookie_error:
                # Log cookie loading error
                monitor.log_event(f"Cookie loading failed: {cookie_error}")

            # Only login with credentials if cookie authentication failed
            if not cookie_auth_successful:
                monitor.log_event("Logging in with credentials")
                await client.login(
                    auth_info_1=credentials["username"],
                    auth_info_2=credentials["email"],
                    password=credentials["password"]
                )

                # Save the new cookies for future use
                client.save_cookies('cookies.json')
                monitor.log_event("Successfully authenticated and saved new cookies")

            async for tweet in client.search_tweet(query, product='Latest'):
                monitor.log_event(f"Received tweet: {tweet.text}")

                # Append tweet to CSV file
                async with aiofiles.open(csv_filename, 'a', newline='', encoding='utf-8') as f:
                    tweet_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    await f.write(f"{tweet_time},{tweet.text.replace(',', ' ')}\n")

                yield tweet.text

        except TooManyRequests as e:
            rate_limit_reset_time = datetime.fromtimestamp(e.rate_limit_reset)
            monitor.log_event(f"Rate limit reached. Waiting until {rate_limit_reset_time}")
            # Calculate wait time
            wait_time = rate_limit_reset_time - datetime.now()
            await asyncio.sleep(wait_time.total_seconds() + 5)  # Add 5 seconds as buffer
            # Continue after waiting
            continue


async def file_source(file_path: str, monitor: PipelineMonitor) -> AsyncGenerator[str, None]:
    """Asynchronous file source reading large files line-by-line."""
    try:
        async with aiofiles.open(file_path, mode='r') as f:
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
