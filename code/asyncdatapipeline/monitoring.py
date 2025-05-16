import logging
from typing import Dict
from time import time

class PipelineMonitor:
    """Observer for tracking pipeline metrics and logging events."""
    
    def __init__(self):
        self.metrics = {
            "throughput": 0,
            "latency": [],
            "errors": 0
        }
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='pipeline.log'
        )
        self.logger = logging.getLogger(__name__)

    def log_event(self, message: str):
        self.logger.info(message)

    def log_error(self, message: str):
        self.logger.error(message)
        self.metrics["errors"] += 1

    def track_processing(self, start_time: float):
        latency = time() - start_time
        self.metrics["throughput"] += 1
        self.metrics["latency"].append(latency)

    def get_metrics(self) -> Dict:
        avg_latency = sum(self.metrics["latency"]) / len(self.metrics["latency"]) if self.metrics["latency"] else 0
        return {
            "throughput": self.metrics["throughput"],
            "avg_latency": avg_latency,
            "errors": self.metrics["errors"]
        }