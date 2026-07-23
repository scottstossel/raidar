import logging
from typing import Optional, Any, Dict
from src.config import settings

logger = logging.getLogger(__name__)

try:
    from langfuse import Langfuse
except ImportError:
    Langfuse = None


class LangfuseTracer:
    """Wrapper around Langfuse for tracing LLM calls and agent decisions."""

    def __init__(self):
        if not Langfuse:
            logger.warning("Langfuse not installed; tracing will be disabled")
            self.client = None
        elif not settings.langfuse_public_key or not settings.langfuse_secret_key:
            logger.warning("Langfuse credentials not configured; tracing will be disabled")
            self.client = None
        else:
            self.client = Langfuse(
                public_key=settings.langfuse_public_key,
                secret_key=settings.langfuse_secret_key,
            )

    def trace_llm_call(
        self,
        name: str,
        model: str,
        prompt: str,
        response: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Log an LLM call to Langfuse."""
        if not self.client:
            return

        try:
            self.client.generation(
                name=name,
                model=model,
                input=prompt,
                output=response,
                metadata=metadata or {},
            )
        except Exception as e:
            logger.error(f"Failed to log LLM call to Langfuse: {e}")

    def trace_agent_decision(
        self,
        agent_name: str,
        input_data: str,
        decision: str,
        confidence: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Log an agent decision."""
        if not self.client:
            return

        try:
            meta = metadata or {}
            if confidence is not None:
                meta["confidence"] = confidence
            self.client.event(
                name=f"{agent_name}_decision",
                input=input_data,
                output=decision,
                metadata=meta,
            )
        except Exception as e:
            logger.error(f"Failed to log agent decision to Langfuse: {e}")

    def flush(self):
        """Flush any pending traces."""
        if self.client:
            self.client.flush()


tracer = LangfuseTracer()
