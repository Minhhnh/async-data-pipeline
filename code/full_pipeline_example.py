import asyncio
from datetime import datetime

from code.asyncdatapipeline.destinations import (
    file_destination,
    no_sql_destination,
    sql_destination,
)
from code.asyncdatapipeline.monitoring import PipelineMonitor
from code.asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from code.asyncdatapipeline.sources import api_source, file_source, twitter_source, websocket_source
from code.asyncdatapipeline.transformers import (
    csv_dict_transformer,
    deduplication_transformer,
    nsfw_transformer,
    uppercase_transformer,
)
from huggingface_hub import login


async def main():
    # Configure with multipart processing enabled
    config = PipelineConfig(
        max_concurrent_tasks=5,
        enable_recovery=True,
        checkpoint_path="checkpoints/pipeline_state.json",
        multipart_enabled=True,
        multipart_threshold=50 * 1024 * 1024,  # 50MB threshold
        multipart_chunk_size=2 * 1024 * 1024,  # 2MB chunks
    )

    # Login to Hugging Face Hub
    login(config.huggingface_token)

    # Create monitor reference first
    monitor = PipelineMonitor()

    # Get current date and date 2 years ago for query
    current_date = datetime.now().strftime("%Y-%m-%d")
    two_years_ago = datetime.now().replace(year=datetime.now().year - 2).strftime("%Y-%m-%d")
    QUERY = f'(from:elonmusk) lang:en until:{current_date} since:{two_years_ago}'

    # Helper to capture the monitor from the pipeline
    def set_monitor_and_create_pipeline():
        nonlocal monitor
        pipeline = AsyncDataPipeline(
            sources=[
                ## Twitter Source
                # lambda: twitter_source(config, monitor, QUERY),

                # REST API Source
                lambda: api_source("http://localhost:9001/tweet", monitor),

                # WebSocket Source - new approach using the dedicated WebSocket source
                lambda: websocket_source("ws://localhost:9001/ws", monitor),

                # File source with multipart processing
                lambda: file_source(
                    "inputs/tweets.txt",
                    monitor,
                    multipart_enabled=config.multipart_enabled,
                    chunk_size=config.multipart_chunk_size
                ),
                lambda: file_source(
                    "inputs/tweets.csv",
                    monitor,
                    delimiter=","
                ),
                lambda: file_source(
                    "inputs/tweets.jsonl",
                    monitor,
                    lines_format=True
                ),
                lambda: file_source(
                    "inputs/tweets.parquet",
                    monitor,
                    lines_format=True
                ),

            ],
            transformers=[
                uppercase_transformer(monitor),
                csv_dict_transformer(monitor),
                deduplication_transformer(monitor, capacity=10000, error_rate=0.01),
                nsfw_transformer(monitor),
            ],
            destinations=[
                file_destination(monitor, "outputs/output.json"),
                file_destination(monitor, "outputs/output.txt"),
                file_destination(monitor, "outputs/output.csv"),
                sql_destination(monitor, config.postgres),
                no_sql_destination(monitor, config.mongo),
            ],
            config=config,
        )
        monitor = pipeline.monitor
        return pipeline

    # Start the FastAPI server in the background (for testing purposes)
    try:
        print("Starting pipeline - make sure the FastAPI server is running on port 9001")
        print("You can start it with: python code/fastapi_server.py")

        pipeline = set_monitor_and_create_pipeline()
        await pipeline.run()

    except KeyboardInterrupt:
        print("Pipeline interrupted by user")
    except Exception as e:
        print(f"Error in pipeline: {e}")


if __name__ == "__main__":
    asyncio.run(main())
