import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
load_dotenv()


@dataclass
class PipelineConfig:
    max_concurrent_tasks: int = 10
    retry_attempts: int = 3
    retry_delay: float = 1.0
    use_tls: bool = True
    multipart_threshold: int = 1024 * 1024 * 100  # 100MB for multipart processing
    # Dict with client_id, client_secret, access_token, access_token_secret
    twitter_credentials: dict = {
        "username": os.getenv("TWITTER_USERNAME"),
        "email": os.getenv("TWITTER_EMAIL"),
        "password": os.getenv("TWITTER_PASSWORD")
    }
