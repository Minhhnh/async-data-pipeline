# Woven: Asynchronous Data Pipeline for Twitter/X

A high-performance asynchronous data pipeline built for collecting, processing, and analyzing Twitter/X data with robust error handling and configurable data flow.

## Features

- **Asynchronous Processing**: Built with Python's asyncio for non-blocking I/O
- **Multiple Data Sources**: Twitter/X API, local files (CSV, TXT, JSON, Parquet), REST APIs, and WebSockets
- **Configurable Transformers**: NSFW content filtering, deduplication, uppercase conversion, and CSV dictionary transformation
- **Flexible Destinations**: File outputs (TXT, CSV, JSON), PostgreSQL, MongoDB, and API endpoints
- **Robust Error Handling**: Comprehensive retry mechanisms and logging
- **Checkpoint Recovery**: Automatically resume processing after interruptions
- **Modular Design**: Easy to extend with new sources, transformers, and destinations
- **Performance Optimized**: Support for large file processing with multipart streaming

## Requirements Implementation

The following table outlines how Woven implements both core requirements and nice-to-have features:

| Requirement                               | Implementation                                                                                  | Status     |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------- | ---------- |
| **Asynchronous File I/O and Network I/O** | Built on Python's asyncio with async/await for all I/O operations (files, network)              | ✅ Complete |
| **Multi-source Input Handling**           | Implemented sources for Twitter API, files (CSV, TXT, JSON, Parquet), REST APIs, and WebSockets | ✅ Complete |
| **Data Transformation and Processing**    | Flexible transformer pattern with sequential processing and callable interface                  | ✅ Complete |
| **Multi-destination Output**              | Support for files (TXT, CSV, JSON), databases (PostgreSQL, MongoDB), and API endpoints          | ✅ Complete |
| **Error Handling and Recovery**           | Retry mechanisms with configurable attempts and delays for pipeline components                  | ✅ Complete |
| **Logging**                               | Detailed monitoring system with colorized console output and file logging                       | ✅ Complete |
| **Type Hinting and Documentation**        | Type annotations throughout codebase with generics for flexibility                              | ✅ Complete |
| **Packaging**                             | PEP-518/PEP-621 compliant packaging with pyproject.toml                                         | ✅ Complete |
| **Performance Optimization**              | Configurable concurrency with semaphores to manage system load                                  | ✅ Complete |
| **Monitoring**                            | Metrics for throughput, latency, and error rates with PipelineMonitor                           | ✅ Complete |
| **Data Reprocessing and Recovery**        | Checkpoint-based recovery system with processed_ids tracking                                    | ✅ Complete |
| **Secure Data Transmission**              | TLS/SSL support with custom certificate configuration and payload encryption                    | ✅ Complete |
| **Flexible Configuration Management**     | Environment variables and PipelineConfig dataclass for all settings                             | ✅ Complete |
| **Scalability Considerations**            | Source/transformer/destination separation allowing horizontal scaling                           | ✅ Complete |
| **Multipart Data Processing**             | Smart chunking for large files with configurable threshold and chunk size                       | ✅ Complete |

## Installation

### Prerequisites

- Python 3.12+
- Linux environment
- Docker and Docker Compose (for database services)

### Installing with `uv`

[`uv`](https://github.com/astral-sh/uv) is a fast Python package installer and resolver. Here's how to install Woven using `uv`:

```bash
# 1. Install uv if you don't have it already
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Create a virtual environment with uv
uv venv --python 3.12.5

# 3. Activate the virtual environment
source .venv/bin/activate

# 4. Install dependencies 
# For wheel file
uv pip install dist/woven-0.1.0-py3-none-any.whl
# For native uv
cd code/
uv sync
```

## Configuration
1. Copy the template.env file to .env:
```bash
cd code/ #if didn't change directory to code/
cp template.env .env
```

2. Edit the .env file with your credentials:
```
# Twitter credentials
TWITTER_USERNAME=your_twitter_username
TWITTER_EMAIL=your_twitter_email
TWITTER_PASSWORD=your_twitter_password

# HuggingFace
HUGGINGFACE_TOKEN=your_huggingface_token
...
```

3. Start database services in new terminal:
```bash
cd code/docker
docker compose up -d
```

4. Start mock API in new terminal:
```bash
cd code/
python fastapi_server.py
```

## Usage

### Basic Usage

Run the main processing pipeline:

```bash
cd code/
python full_example_pipeline.py
```

### Testing

Run the pipeline unittests:

```bash
cd code/
pytest tests/
```

### Architecture

The pipeline follows a modular architecture with three main components:

1. **Sources**: Generate data from various inputs (Twitter, files, APIs)
2. **Transformers**: Process data through sequential transformations
3. **Destinations**: Output processed data to files, databases, or APIs

Each component is designed to work asynchronously and can be easily extended with new implementations.

### Custom Usage Examples

#### 1. Twitter Data Collection

```python
import asyncio
from asyncdatapipeline.sources import twitter_source
from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.config import PipelineConfig

async def collect_tweets():
    config = PipelineConfig()
    monitor = PipelineMonitor()
    query = "artificial intelligence"
    
    async for tweet in twitter_source(config, monitor, query=query)():
        print(f"Collected tweet: {tweet['text'][:50]}...")
        
asyncio.run(collect_tweets())
```

#### 2. Custom Pipeline with File Source and NSFW Filtering

```python
import asyncio
from asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from asyncdatapipeline.sources import file_source
from asyncdatapipeline.transformers import nsfw_transformer, uppercase_transformer
from asyncdatapipeline.destinations import file_destination

async def main():
    config = PipelineConfig(max_concurrent_tasks=5)
    monitor = PipelineMonitor()
    
    pipeline = AsyncDataPipeline(
        sources=[
            lambda: file_source("inputs/tweets.txt", monitor)
        ],
        transformers=[
            uppercase_transformer(monitor),
            nsfw_transformer(monitor, threshold=0.7)
        ],
        destinations=[
            file_destination(monitor, "outputs/filtered_tweets.json")
        ],
        config=config
    )
    
    await pipeline.run()
    
asyncio.run(main())
```

#### 3. Database Storage Pipeline

```python
import asyncio
from asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from asyncdatapipeline.sources import api_source
from asyncdatapipeline.destinations import sql_destination, no_sql_destination
from asyncdatapipeline.monitoring import PipelineMonitor

async def store_to_databases():
    config = PipelineConfig()
    monitor = PipelineMonitor()
    
    pipeline = AsyncDataPipeline(
        sources=[
            lambda: api_source("http://localhost:9001/data", monitor)
        ],
        transformers=[],
        destinations=[
            sql_destination(monitor, config.postgres),
            no_sql_destination(monitor, config.mongo)
        ],
        config=config
    )
    
    await pipeline.run()
    
asyncio.run(store_to_databases())
```

## Security Features

### Secure Data Transmission

Woven implements several security features to ensure data is transmitted securely:

1. **TLS/SSL Support**: All network connections use TLS/SSL encryption by default
   - API requests are made over HTTPS
   - WebSocket connections use WSS (WebSocket Secure)
   - Certificate validation is enabled by default

2. **Custom Certificate Support**: You can provide your own certificates for enhanced security
   - Set `ssl_cert_path` in the PipelineConfig

3. **Payload Encryption**: Optional end-to-end encryption of data payloads
   - Enable with `enable_payload_encryption=True` in PipelineConfig
   - Uses Fernet symmetric encryption (AES-128 in CBC mode with PKCS7 padding)

4. **Secure Environment Variables**: Sensitive credentials are loaded from environment variables
   - Supports `.env` file for local development
   - Credentials are never hardcoded

### Security Configuration Example

```python
from asyncdatapipeline.pipeline import AsyncDataPipeline
from asyncdatapipeline.config import PipelineConfig

# Configure with enhanced security
config = PipelineConfig(
    use_tls=True,                        # Use TLS/SSL for connections (default: True)
    verify_ssl=True,                     # Verify SSL certificates (default: True)
    ssl_cert_path="/path/to/cert.pem",   # Optional custom certificate 
    enable_payload_encryption=True,      # Enable payload encryption (default: False)
)

# Create pipeline with secure configuration
pipeline = AsyncDataPipeline(
    # ...sources, transformers, destinations...
    config=config
)
```

## Design Decisions

### Asynchronous Architecture

The pipeline is built on Python's asyncio to maximize throughput when dealing with I/O-bound operations like network requests and file handling. This allows for efficient concurrent processing without the overhead of threads or processes.

### Retry Mechanism

The pipeline implements a configurable retry mechanism with:
- Configurable retry attempts via `retry_attempts` (default: 3)
- Configurable delay between retries via `retry_delay` (default: 1.0 seconds)
- Intelligent retry for all pipeline components (sources, transformers, destinations)
- Detailed logging of retry attempts and failures

### Pipeline Components

1. **Sources**
   - Abstract base class with async generators for data acquisition
   - Implementations for Twitter/X API, files (various formats), REST APIs, and WebSockets
   - Support for chunked processing of large files with multipart capability

2. **Transformers**
   - **NSFW Content Filter**: Essential for removing inappropriate or harmful content from Twitter/X streams to maintain data quality and compliance
   - **Deduplication**: Uses space-efficient Bloom filters to prevent duplicate content, critical for accurate data analysis and storage optimization
   - **Format Conversion**: Transforms data between formats (CSV, JSON, dictionaries) for consistent pipeline processing
   - **Text Transformation**: Simple uppercase transformation

3. **Destinations**
   - Async writers for storing processed data
   - Multiple destination support for redundancy and different analysis needs
   - Database connections (PostgreSQL, MongoDB) with efficient connection pooling

The modular design allows components to be combined in various ways, with each component focused on a single responsibility. This approach simplifies testing, enables reuse, and provides flexibility to adapt to different data processing needs.

### Error Handling and Recovery

- Comprehensive error logging with colorized output
- Context-specific error messages
- Checkpoint system to restart processing after interruptions

### Monitoring and Metrics

- Runtime metrics for throughput and latency
- Detailed logging with multiple levels
- Error tracking for issue diagnosis

## Limitations and Edge Cases

1. **Rate Limiting**
   - Twitter API has strict rate limits that may pause collection
   - The pipeline implements exponential backoff, but long pauses may still occur

2. **Memory Usage**
   - Large datasets can consume significant memory
   - Use the multipart processing feature for very large files

3. **Authentication**
   - Twitter credentials may expire or be rejected
   - Cookie-based auth helps but may still require periodic re-authentication

4. **Data Quality**
   - Twitter content may contain unexpected formats or characters
   - The pipeline includes sanitization but edge cases may occur

5. **Resource Constraints**
   - Database connections should be monitored when scaling up
   - Consider sharding for very large datasets

## Advanced Configuration

### Retry Configuration

To customize retry behavior for handling transient errors:

```python
config = PipelineConfig(
    retry_attempts=5,    # Number of retry attempts (default: 3)
    retry_delay=2.0      # Delay between retries in seconds (default: 1.0)
)
```

### Multipart Processing

For handling extremely large files, adjust the following in the PipelineConfig:

```python
config = PipelineConfig(
    multipart_enabled=True,
    multipart_threshold=100 * 1024 * 1024,  # 100MB threshold
    multipart_chunk_size=2 * 1024 * 1024,   # 2MB chunks
)
```

### Checkpoint Frequency

To adjust how often the processing state is saved:

```python
config = PipelineConfig(
    enable_recovery=True,
    checkpoint_frequency=500,  # Save every 500 items
    checkpoint_path="custom/path/checkpoint.json"
)
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
