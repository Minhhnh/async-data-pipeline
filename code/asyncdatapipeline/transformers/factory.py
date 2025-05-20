from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.transformers.nsfw import NSFWTransformer
from asyncdatapipeline.transformers.text import (
    UppercaseTransformer,
    DeduplicateTransformer,
    CSVToDictTransformer
)


def uppercase_transformer(monitor: PipelineMonitor):
    """Factory function for creating uppercase transformer."""
    return UppercaseTransformer(monitor)


def nsfw_transformer(monitor: PipelineMonitor, threshold: float = 0.5):
    """Factory function for creating NSFW content transformer."""
    return NSFWTransformer(monitor, threshold=threshold)


def deduplication_transformer(monitor: PipelineMonitor, capacity: int = 10000, error_rate: float = 0.01):
    """Factory function for creating deduplication transformer."""
    return DeduplicateTransformer(monitor, capacity=capacity, error_rate=error_rate)


def csv_dict_transformer(monitor: PipelineMonitor):
    """Factory function for creating CSV to dict transformer."""
    return CSVToDictTransformer(monitor)
