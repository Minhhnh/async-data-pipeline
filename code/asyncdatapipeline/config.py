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

    # Recovery and checkpointing
    enable_recovery: bool = True
    checkpoint_path: str = "checkpoints/pipeline_state.json"
    checkpoint_frequency: int = 100  # Save checkpoint every N items

    # Multipart processing settings
    multipart_threshold: int = 1024 * 1024 * 100  # 100MB for multipart processing
    multipart_chunk_size: int = 1024 * 1024  # 1MB chunks for multipart processing
    multipart_enabled: bool = True  # Enable multipart processing by default

    # Dict with username, email, password
    twitter_credentials: dict = field(default_factory=_get_twitter_credentials)
    postgres: dict = field(default_factory=_get_postgres_info)
    mongo: dict = field(default_factory=_get_mongo_info)
    huggingface_token: Optional[str] = os.getenv("HUGGINGFACE_TOKEN")

    # Additional settings
    cookie_path: str = "cookies.json"
    max_concurrent_requests: int = 1

    # Security settings
    verify_ssl: bool = True  # Verify SSL certificates
    ssl_cert_path: Optional[str] = None  # Path to custom SSL certificate
    enable_payload_encryption: bool = False  # Enable additional payload encryption
    encryption_key: Optional[str] = os.getenv("ENCRYPTION_KEY")  # Key for payload encryption
