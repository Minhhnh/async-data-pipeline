import asyncio
from datetime import datetime

from asyncdatapipeline.destinations import (file_destination,
                                            no_sql_destination,
                                            sql_destination)
from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from asyncdatapipeline.sources import api_source, file_source, twitter_source
from asyncdatapipeline.transformers import (transform_csv_line_to_dict,
                                            uppercase_transformer)


async def main():
    config = PipelineConfig(max_concurrent_tasks=5)

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
                # lambda: twitter_source(config.twitter_credentials, monitor, query=QUERY),
                lambda: file_source("inputs/tweets.csv", monitor),
                # lambda: api_source("https://api.example.com/data", monitor, locale="ja_JP", max_items=100),
                # lambda: file_source("inputs/tweets.txt", monitor),
            ],
            transformers=[
                lambda x: uppercase_transformer(x, monitor),
                lambda x: transform_csv_line_to_dict(x, monitor)
            ],
            destinations=[
                # file_destination(monitor, "outputs/output.txt"),
                # file_destination(monitor, "outputs/output.csv"),
                # file_destination(monitor, "outputs/output.json"),
                # sql_destination(monitor, config.postgres),
                no_sql_destination(monitor, config.mongo),
            ],
            config=config,
        )
        monitor = pipeline.monitor
        return pipeline

    pipeline = set_monitor_and_create_pipeline()
    await pipeline.run()


asyncio.run(main())
