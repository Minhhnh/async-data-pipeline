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


def _get_postgres_info() -> dict:
    """Factory function to create postgres credentials dictionary."""
    return {
        "name": "postgres",
        "table_name": "tweets",
        "credentials": {
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "database": os.getenv("POSTGRES_DB")
        },
        "columns": ["timestamp", "username", "text", "created_at", "retweets", "likes"],
    }


def _get_mongo_info() -> dict:
    """Factory function to create mongo credentials dictionary."""
    return {
        "name": "mongo",
        "collection_name": "tweets",
        "credentials": {
            "host": os.getenv("MONGO_HOST", "localhost"),
            "port": int(os.getenv("MONGO_PORT", 27017)),
            "database": os.getenv("MONGO_DB", "twitter_db"),
            "user": os.getenv("MONGO_ROOT_USER"),
            "password": os.getenv("MONGO_ROOT_PASSWORD"),
        }
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
    postgres: dict = field(default_factory=_get_postgres_info)
    mongo: dict = field(default_factory=_get_mongo_info)
