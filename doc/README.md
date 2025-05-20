# Woven: Asynchronous Data Pipeline for Twitter/X

A high-performance asynchronous data pipeline built for collecting, processing, and analyzing Twitter/X data with robust error handling and configurable data flow.

## Features

- **Asynchronous Processing**: Built with Python's asyncio for non-blocking I/O
- **Multiple Data Sources**: Twitter/X API, local files (CSV, TXT), and mock API sources
- **Configurable Transformers**: NSFW content filtering, deduplication, and formatting
- **Flexible Destinations**: File outputs (TXT, CSV, JSON), PostgreSQL, MongoDB
- **Robust Error Handling**: Comprehensive retry mechanisms and logging
- **Checkpoint Recovery**: Automatically resume processing after interruptions
- **Modular Design**: Easy to extend with new sources, transformers, and destinations
- **Performance Optimized**: Support for large file processing with multipart streaming

## Requirements Implementation

The following table outlines how Woven implements both core requirements and nice-to-have features:

| Requirement | Implementation | Status |
|-------------|---------------|--------|
| **Asynchronous I/O** | Built on Python's asyncio with async/await for all I/O operations | ✅ Complete |
| **Multi-source Input** | Implemented sources for Twitter API, files (CSV, TXT), and APIs | ✅ Complete |
| **Data Transformation** | Transformer pattern with sequential processing and flexible type handling | ✅ Complete |
| **Multi-destination Output** | Support for files, databases (PostgreSQL, MongoDB), and API endpoints | ✅ Complete |
| **Error Handling** | Comprehensive exception handling with retry mechanisms | ✅ Complete |
| **Logging** | Detailed logging with colorized console output and file logging | ✅ Complete |
| **Type Hinting** | Type annotations throughout codebase with generics for flexibility | ✅ Complete |
| **Documentation** | PEP-257 compliant docstrings and comprehensive README | ✅ Complete |
| **Packaging** | PEP-518/PEP-621 compliant packaging with pyproject.toml | ✅ Complete |
| **Performance Optimization** | Configurable concurrency settings to manage system load | ✅ Complete |
| **Monitoring** | Metrics for throughput, latency, and error rates | ✅ Complete |
| **Data Recovery** | Checkpoint-based recovery system for interrupted processing | ✅ Complete |
| **Secure Transmission** | TLS/SSL support for API communications | ✅ Complete |
| **Flexible Configuration** | Environment variables and config objects for all settings | ✅ Complete |
| **Scalability** | Designed for horizontal scaling with independent components | ✅ Complete |
| **Multipart Processing** | Smart chunking for large files with size-based activation | ✅ Complete |

## Installation

### Prerequisites

- Python 3.12+
- Linux kernel 6.x environment
- Docker and Docker Compose (for database services)

### Installing with `uv`

[`uv`](https://github.com/astral-sh/uv) is a fast Python package installer and resolver. Here's how to install Woven using `uv`:

```bash
# 1. Install uv if you don't have it already
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Change to code directory
cd code

# 3. Create a virtual environment with uv
uv venv --python 3.12.5

# 4. Activate the virtual environment
source .venv/bin/activate

# 5. Install dependencies using uv
uv sync
```

## Configuration

1. Copy the template.env file to .env:
```bash
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

3. (Optional) Start database services if needed:
```bash
cd docker
docker-compose up -d
```

## Usage

### Basic Usage

Run the main processing pipeline:

```bash
python main.py
```

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
    
    async for tweet in twitter_source(config, monitor, query=query):
        print(f"Collected tweet: {tweet['text'][:50]}...")
        
asyncio.run(collect_tweets())
```

#### 2. Custom Pipeline with File Source and NSFW Filtering

```python
import asyncio
from asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from asyncdatapipeline.sources import file_source
from asyncdatapipeline.transformers import nsfw_transformer, csv_dict_transformer
from asyncdatapipeline.destinations import file_destination

async def main():
    config = PipelineConfig(max_concurrent_tasks=5)
    
    pipeline = AsyncDataPipeline(
        sources=[
            lambda: file_source("inputs/tweets.csv", PipelineMonitor())
        ],
        transformers=[
            csv_dict_transformer(PipelineMonitor()),
            nsfw_transformer(PipelineMonitor(), threshold=0.7)
        ],
        destinations=[
            file_destination(PipelineMonitor(), "outputs/filtered_tweets.json")
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
from asyncdatapipeline.sources import twitter_source
from asyncdatapipeline.destinations import sql_destination, no_sql_destination
from asyncdatapipeline.monitoring import PipelineMonitor

async def store_to_databases():
    config = PipelineConfig()
    monitor = PipelineMonitor()
    
    pipeline = AsyncDataPipeline(
        sources=[
            lambda: twitter_source(config, monitor, query="data science")
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

## Design Decisions

### Asynchronous Architecture

The pipeline is built on Python's asyncio to maximize throughput when dealing with I/O-bound operations like network requests and file handling. This allows for efficient concurrent processing without the overhead of threads or processes.

### Pipeline Components

1. **Sources**
   - Abstract base class with async generators
   - Specialized implementations for Twitter, files, and APIs (Because of there is no API here, I am using faker to generate data.)
   - Support for chunked processing of large files

2. **Transformers**
   - Pure functions for data transformation
   - Content filtering including NSFW detection
   - Support for both simple and complex transformations

3. **Destinations**
   - Async writers for various output formats
   - Database connections are managed efficiently
   - Batching support for database operations

### Error Handling and Recovery

- Comprehensive error logging with colorized output
- Automatic retry mechanism for transient failures
- Checkpoint system to restart processing after crashes

### Monitoring and Metrics

- Runtime metrics for throughput and latency
- Detailed logging with multiple levels
- Performance tracking for bottleneck identification

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
