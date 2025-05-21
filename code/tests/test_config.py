import os
import pytest
from unittest.mock import patch

from asyncdatapipeline.config import (
    PipelineConfig,
    _get_twitter_credentials,
    _get_postgres_info,
    _get_mongo_info
)


def test_config_defaults():
    """Test that default configuration values are set correctly."""
    config = PipelineConfig()

    # Check default values
    assert config.max_concurrent_tasks == 10
    assert config.retry_attempts == 3
    assert config.retry_delay == 1.0
    assert config.use_tls is True
    assert config.enable_recovery is True
    assert config.checkpoint_path == "checkpoints/pipeline_state.json"
    assert config.checkpoint_frequency == 100
    assert config.multipart_threshold == 1024 * 1024 * 100  # 100MB
    assert config.multipart_chunk_size == 1024 * 1024  # 1MB
    assert config.multipart_enabled is True
    assert config.cookie_path == "cookies.json"
    assert config.max_concurrent_requests == 1
    assert config.verify_ssl is True
    assert config.ssl_cert_path is None
    assert config.enable_payload_encryption is False


def test_config_custom_values():
    """Test setting custom configuration values."""
    custom_config = PipelineConfig(
        max_concurrent_tasks=5,
        retry_attempts=2,
        retry_delay=2.0,
        use_tls=False,
        enable_recovery=False,
        checkpoint_path="custom/path.json",
        checkpoint_frequency=50,
        multipart_enabled=False,
        verify_ssl=False,
        enable_payload_encryption=True
    )

    # Check custom values
    assert custom_config.max_concurrent_tasks == 5
    assert custom_config.retry_attempts == 2
    assert custom_config.retry_delay == 2.0
    assert custom_config.use_tls is False
    assert custom_config.enable_recovery is False
    assert custom_config.checkpoint_path == "custom/path.json"
    assert custom_config.checkpoint_frequency == 50
    assert custom_config.multipart_enabled is False
    assert custom_config.verify_ssl is False
    assert custom_config.enable_payload_encryption is True


@patch.dict(os.environ, {
    "TWITTER_USERNAME": "test_user",
    "TWITTER_EMAIL": "test@example.com",
    "TWITTER_PASSWORD": "test_pass"
})
def test_twitter_credentials():
    """Test Twitter credentials are loaded correctly from environment."""
    credentials = _get_twitter_credentials()

    assert credentials["username"] == "test_user"
    assert credentials["email"] == "test@example.com"
    assert credentials["password"] == "test_pass"


@patch.dict(os.environ, {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_USER": "postgres",
    "POSTGRES_PASSWORD": "postgres",
    "POSTGRES_DB": "test_db"
})
def test_postgres_info():
    """Test Postgres information is loaded correctly."""
    postgres_info = _get_postgres_info()

    assert postgres_info["name"] == "postgres"
    assert postgres_info["table_name"] == "tweets"
    assert postgres_info["credentials"]["host"] == "localhost"
    assert postgres_info["credentials"]["port"] == "5432"
    assert postgres_info["credentials"]["user"] == "postgres"
    assert postgres_info["credentials"]["password"] == "postgres"
    assert postgres_info["credentials"]["database"] == "test_db"
    assert "columns" in postgres_info


@patch.dict(os.environ, {
    "MONGO_HOST": "localhost",
    "MONGO_PORT": "27017",
    "MONGO_DB": "test_mongo_db",
    "MONGO_ROOT_USER": "admin",
    "MONGO_ROOT_PASSWORD": "password"
})
def test_mongo_info():
    """Test MongoDB information is loaded correctly."""
    mongo_info = _get_mongo_info()

    assert mongo_info["name"] == "mongo"
    assert mongo_info["collection_name"] == "tweets"
    assert mongo_info["credentials"]["host"] == "localhost"
    assert mongo_info["credentials"]["port"] == 27017
    assert mongo_info["credentials"]["database"] == "test_mongo_db"
    assert mongo_info["credentials"]["user"] == "admin"
    assert mongo_info["credentials"]["password"] == "password"


def test_config_factory_functions():
    """Test that factory functions are called when creating a config."""
    config = PipelineConfig()

    # Check that dictionaries were created
    assert isinstance(config.twitter_credentials, dict)
    assert isinstance(config.postgres, dict)
    assert isinstance(config.mongo, dict)
