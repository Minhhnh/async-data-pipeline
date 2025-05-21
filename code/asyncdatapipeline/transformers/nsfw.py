"""NSFW detector transformer implementation."""

import asyncio
from typing import Any, Dict, Optional

import torch
from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.transformers.base import BaseTransformer

from transformers.modeling_utils import PreTrainedModel
from transformers.models.auto.modeling_auto import AutoModelForSequenceClassification
from transformers.models.auto.tokenization_auto import AutoTokenizer
from transformers.tokenization_utils import PreTrainedTokenizer


class NSFWTransformer(BaseTransformer):
    """Transformer for detecting and filtering NSFW content using qiuhuachuan/NSFW-detector."""

    def __init__(self, monitor: PipelineMonitor, model_name: str = "eliasalbouzidi/distilbert-nsfw-text-classifier", threshold: float = 0.5):
        """
        Initialize the NSFW transformer.

        Args:
            threshold: Threshold for NSFW classification (0.0-1.0).
            monitor: Pipeline monitor instance. If None, a monitor must be provided during call.
        """
        super().__init__(monitor)
        self.threshold = threshold
        self.model_name = model_name
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer: PreTrainedTokenizer
        self.model: PreTrainedModel
        self._model_loaded = False
        self._load_model()

    def _load_model(self) -> None:
        """Load model and tokenizer."""
        if self._model_loaded:
            return

        if not self.monitor:
            raise ValueError("Monitor must be provided either during initialization or call")

        try:
            self.monitor.log_event(f"Loading NSFW detector model: {self.model_name}")
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, use_fast=True)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self.model.to(self.device)
            self.model.eval()
            self._model_loaded = True
            self.monitor.log_event("NSFW model loaded successfully")
        except Exception as e:
            self.monitor.log_error(f"Failed to load NSFW model: {e}")
            raise

    async def detect_nsfw(self, text: str) -> Optional[Dict[str, float]]:
        """
        Detect NSFW content in text asynchronously.

        Args:
            text: Input text to analyze.

        Returns:
            Dictionary with NSFW probability scores or None on error.
        """
        if not self._model_loaded:
            self._load_model()

        try:
            inputs = self.tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=512,
                padding="max_length"
            )
            inputs = {k: v.to(self.device) for k, v in inputs.items()}

            # Run inference in thread pool
            loop = asyncio.get_running_loop()

            def infer() -> torch.Tensor:
                with torch.no_grad():
                    outputs = self.model(**inputs)
                    probs = torch.softmax(outputs.logits, dim=-1)
                    return probs[0].cpu().numpy()
            probs = await loop.run_in_executor(None, infer)

            # Map to labels (0: safe, 1: nsfw)
            labels = ["safe", "nsfw"]
            result = {label: float(prob) for label, prob in zip(labels, probs)}
            return result
        except Exception as e:
            self.monitor.log_error(f"NSFW detection failed: {e}")
            raise

    async def transform(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform and filter NSFW content.

        Args:
            data: Input data (dict with Text field or string).

        Returns:
            Original data with NSFW metadata or None if filtered.
        """
        try:
            # Extract text
            text = self._get_key(data)
            if not isinstance(text, str) or not text.strip():
                self.monitor.log_warning(f"Cannot analyze non-string or empty content: {type(text)}")
                return data

            # Detect NSFW
            result = await self.detect_nsfw(text)
            nsfw_score = result.get("nsfw", 0.0)

            if nsfw_score >= self.threshold:
                self.monitor.log_event(
                    f"Filtered NSFW tweet (score: {nsfw_score:.2f}) with content: {text[:50]}...")
                return None
            return data
        except Exception as e:
            self.monitor.log_error(f"NSFW transformer error: {e}")
            raise
