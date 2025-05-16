import asyncio
from asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from asyncdatapipeline.sources import twitter_source, file_source
from asyncdatapipeline.transformers import uppercase_transformer
from asyncdatapipeline.destinations import file_destination
from asyncdatapipeline.config import TwitterCredentials


async def main():
    config = PipelineConfig(
        max_concurrent_tasks=5,
        twitter_credentials=TwitterCredentials()
    )
    pipeline = AsyncDataPipeline(
        sources=[
            lambda: twitter_source(config.twitter_credentials, pipeline.monitor, "#tech"),
            lambda: file_source('input.log', pipeline.monitor)
        ],
        transformers=[lambda x: uppercase_transformer(x, pipeline.monitor)],
        destinations=[lambda x: file_destination(x, pipeline.monitor)],
        config=config
    )
    await pipeline.run()

asyncio.run(main())
