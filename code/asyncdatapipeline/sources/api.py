"""API data source implementation."""

import asyncio
from typing import AsyncGenerator, Dict, Any

from faker import Faker
import aiohttp

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.sources.base import Source


class ApiSource(Source):
    """API source generating fake JSON data using Faker."""

    def __init__(
        self,
        url: str,
        monitor: PipelineMonitor,
        locale: str = "en_US",
        max_items: int = 100,
    ):
        super().__init__(monitor)
        self.url = url  # Used as identifier or config (e.g., data type)
        self.faker = Faker(locale)
        self.max_items = max_items

    async def generate(self) -> AsyncGenerator[dict, None]:
        """Generate fake API data using Faker."""
        count = 0
        try:
            while self.max_items is None or count < self.max_items:
                # Generate fake user profile
                item = {
                    "timestamp": self.faker.date_time_this_decade().isoformat(),
                    "username": self.faker.user_name(),
                    "text": self.faker.sentence(),
                    "created_at": self.faker.date_time_this_decade().isoformat(),
                    "retweets": self.faker.random_int(min=0, max=1000),
                    "likes": self.faker.random_int(min=0, max=1000),
                }

                self.monitor.log_event(f"Receive item for {self.url}")
                yield item
                count += 1
                # Simulate API latency
                await asyncio.sleep(0.1)  # Adjust for desired stream rate
        except Exception as e:
            self.monitor.log_error(f"Error generating fake data for {self.url}: {e}")
            raise
