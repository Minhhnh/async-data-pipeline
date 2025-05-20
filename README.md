# Woven

## Overview
This project is an asynchronous data pipeline framework designed for Twitter/X data collection and processing. It provides a modular, high-performance system for collecting, transforming, and storing social media data using modern Python async capabilities.

## Features
- **Asynchronous Processing**: Fully asynchronous architecture for high-throughput data processing
- **Modular Pipeline**: Pluggable sources, transformers, and destinations for maximum flexibility
- **Built-in Twitter Integration**: Ready-to-use Twitter/X data collection using twikit
- **Content Moderation**: NSFW content detection using Hugging Face models
- **Data Deduplication**: Memory-efficient deduplication using Bloom filters
- **Multiple Destinations**: Support for file output, PostgreSQL, MongoDB, and API endpoints
- **Monitoring**: Comprehensive event logging and performance metrics
- **Containerization**: Ready-to-use Docker configuration for databases

## Architecture
This project follows a clean, modular architecture with three main components:

1. **Sources**: Where data comes from (Twitter/X, files, APIs)
2. **Transformers**: Process and modify data (filter, convert, deduplicate)
3. **Destinations**: Where data is sent (files, databases, APIs)

``` 
┌─────────┐     ┌─────────────┐     ┌─────────────┐
│ Sources │ ──► │Transformers │ ──► │Destinations │
└─────────┘     └─────────────┘     └─────────────┘
    │                 │                   │
    ▼                 ▼                   ▼
┌─────────┐     ┌─────────────┐     ┌─────────────┐
│ Twitter │     │ Uppercase   │     │ Files       │
│ Files   │     │ NSFW Filter │     │ PostgreSQL  │
│ APIs    │     │ Deduplicate │     │ MongoDB     │
└─────────┘     └─────────────┘     └─────────────┘
```

## Installation

### Prerequisites
- Python 3.12+
- Docker and Docker Compose (for database support)
- Twitter/X credentials (for Twitter data collection)
- Hugging Face account (for NSFW detection models)

### Environment Setup
1. Clone the repository:
```bash
git clone https://github.com/yourusername/Woven.git
cd Woven
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
# Or, for development:
pip install -e .
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your credentials
```

### Docker Setup
For database support, start the Docker containers:
```bash
cd code/docker
docker-compose up -d
```

This starts PostgreSQL and MongoDB containers with web management interfaces.

## Usage

### Basic Example
```python
import asyncio
from asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from asyncdatapipeline.sources import file_source
from asyncdatapipeline.transformers import uppercase_transformer
from asyncdatapipeline.destinations import file_destination
from asyncdatapipeline.monitoring import PipelineMonitor

async def main():
    # Create monitor and configuration
    monitor = PipelineMonitor()
    config = PipelineConfig(max_concurrent_tasks=5)
    
    # Create pipeline
    pipeline = AsyncDataPipeline(
        sources=[
            lambda: file_source("inputs/data.csv", monitor)
        ],
        transformers=[
            uppercase_transformer(monitor)
        ],
        destinations=[
            file_destination(monitor, "outputs/output.txt")
        ],
        config=config
    )
    
    # Run the pipeline
    await pipeline.run()

if __name__ == "__main__":
    asyncio.run(main())
```

### Twitter Data Collection
```python
import asyncio
from asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from asyncdatapipeline.sources import twitter_source
from asyncdatapipeline.transformers import nsfw_transformer, deduplication_transformer
from asyncdatapipeline.destinations import file_destination, sql_destination
from asyncdatapipeline.monitoring import PipelineMonitor

async def main():
    # Load configuration
    config = PipelineConfig()
    monitor = PipelineMonitor()
    
    # Define Twitter query
    query = '(from:elonmusk) lang:en until:2025-05-16 since:2023-05-16'
    
    # Create pipeline
    pipeline = AsyncDataPipeline(
        sources=[
            lambda: twitter_source(config.twitter_credentials, monitor, query=query)
        ],
        transformers=[
            deduplication_transformer(monitor, capacity=10000, error_rate=0.01),
            nsfw_transformer(monitor, threshold=0.7)
        ],
        destinations=[
            file_destination(monitor, "outputs/tweets.csv"),
            sql_destination(monitor, config.postgres)
        ],
        config=config
    )
    
    # Run the pipeline
    await pipeline.run()

if __name__ == "__main__":
    asyncio.run(main())
```

## Components

### Sources
- **TwitterSource**: Collects tweets from Twitter/X API
- **FileSource**: Reads data from local files
- **CSVFileSource**: Specialized for reading CSV files
- **ApiSource**: Fetches data from REST APIs

### Transformers
- **UppercaseTransformer**: Converts text to uppercase
- **CSVToDictTransformer**: Converts CSV lines to dictionaries
- **DeduplicateTransformer**: Removes duplicate data using Bloom filters
- **NSFWTransformer**: Detects and filters inappropriate content

### Destinations
- **FileDestination**: Writes data to regular files
- **CSVFileDestination**: Specialized for CSV output
- **JSONFileDestination**: Specialized for JSON output
- **PostgreSQLDestination**: Stores data in PostgreSQL
- **MongoDBDestination**: Stores data in MongoDB
- **ApiDestination**: Sends data to REST APIs

## Configuration
The system uses a centralized configuration object with these parameters:

```python
@dataclass
class PipelineConfig:
    max_concurrent_tasks: int = 10
    retry_attempts: int = 3
    retry_delay: float = 1.0
    use_tls: bool = True
    twitter_credentials: dict = {...}
    postgres: dict = {...}
    mongo: dict = {...}
    huggingface_token: Optional[str] = None
```

## Docker Integration
The project includes Docker Compose configuration for:
- PostgreSQL database
- MongoDB database
- Mongo Express web interface

You can view your data in the Mongo Express interface at http://localhost:8081

## Development

### Project Structure
```
Woven/
├── code/
│   ├── asyncdatapipeline/     # Main library code
│   │   ├── sources/           # Data sources
│   │   ├── transformers/      # Data transformers
│   │   ├── destinations/      # Data destinations 
│   │   ├── monitoring.py      # Logging and metrics
│   │   ├── pipeline.py        # Core pipeline implementation
│   │   └── config.py          # Configuration handling
│   ├── docker/                # Docker configuration
│   ├── tests/                 # Test suites
│   └── main.py                # Example application
├── requirements.txt           # Dependencies
├── pyproject.toml             # Project metadata
└── README.md                  # This file
```

### Adding New Components

#### Creating a New Source
```python
from asyncdatapipeline.sources.base import Source
from asyncdatapipeline.monitoring import PipelineMonitor

class MyCustomSource(Source):
    def __init__(self, config, monitor: PipelineMonitor):
        super().__init__(monitor)
        self.config = config
        
    async def generate(self):
        # Implement your data generation logic here
        for item in range(10):
            yield {"data": item}
```

#### Creating a New Transformer
```python
from asyncdatapipeline.transformers.base import BaseTransformer
from asyncdatapipeline.monitoring import PipelineMonitor

class MyCustomTransformer(BaseTransformer):
    def __init__(self, monitor: PipelineMonitor):
        super().__init__(monitor)
        
    async def transform(self, data):
        # Implement your transformation logic
        data["processed"] = True
        return data
```

#### Creating a New Destination
```python
from asyncdatapipeline.destinations.base import Destination
from asyncdatapipeline.monitoring import PipelineMonitor

class MyCustomDestination(Destination):
    def __init__(self, config, monitor: PipelineMonitor):
        super().__init__(monitor)
        self.config = config
        
    async def send(self, data):
        # Implement your data storage logic
        print(f"Storing data: {data}")
```

## Testing
Run the test suite with:

```bash
pytest code/tests/
```

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Contributors
- Your Name <your.email@example.com>

## Acknowledgments
- Twitter API/X API for data access
- Hugging Face for machine learning models
- twikit for Twitter integration
