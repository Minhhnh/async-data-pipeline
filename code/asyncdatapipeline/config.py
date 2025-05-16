import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv
load_dotenv()


def _get_twitter_credentials() -> dict:
    """Factory function to create twitter credentials dictionary."""
    return {
        "username": os.getenv("TWITTER_USERNAME"),
        "email": os.getenv("TWITTER_EMAIL"),
        "password": os.getenv("TWITTER_PASSWORD")
    }


@dataclass
class PipelineConfig:
    max_concurrent_tasks: int = 10
    retry_attempts: int = 3
    retry_delay: float = 1.0
    use_tls: bool = True
    multipart_threshold: int = 1024 * 1024 * 100  # 100MB for multipart processing
    # Dict with username, email, password
    twitter_credentials: dict = field(default_factory=_get_twitter_credentials)
