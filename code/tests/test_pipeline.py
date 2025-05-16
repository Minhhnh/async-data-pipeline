import pytest
import asyncio
from asyncdatapipeline.pipeline import AsyncDataPipeline, PipelineConfig
from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.transformers import uppercase_transformer

@pytest.mark.asyncio
async def test_pipeline():
    async def mock_source():
        yield "test tweet"
    
    async def mock_destination(data: str, monitor: PipelineMonitor):
        assert data == "TEST TWEET"
    
    pipeline = AsyncDataPipeline(
        sources=[mock_source],
        transformers=[uppercase_transformer],
        destinations=[mock_destination],
        config=PipelineConfig(max_concurrent_tasks=1)
    )
    await pipeline.run()
    assert pipeline.monitor.metrics["throughput"] == 1