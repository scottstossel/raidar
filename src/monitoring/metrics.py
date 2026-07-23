"""Metric definitions for observability."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class IngestionMetrics:
    """Metrics tracked during ingestion."""

    source: str
    fetch_count: int
    fetch_errors: int
    dedup_matches: int
    new_documents: int
    timestamp: datetime


@dataclass
class FeatureMetrics:
    """Metrics for feature computation stages."""

    stage: str  # 'topic_tagging', 'embeddings', 'trend_scoring', 'themes'
    processed_count: int
    success_count: int
    error_count: int
    avg_latency_seconds: float
    timestamp: datetime


@dataclass
class AgentMetrics:
    """Metrics for intel agents."""

    agent_name: str  # 'discovery', 'analysis', 'skeptic', 'synthesis'
    call_count: int
    success_count: int
    error_count: int
    total_cost_usd: float
    avg_latency_seconds: float
    timestamp: datetime


def log_ingestion_metric(
    source: str,
    fetch_count: int,
    fetch_errors: int,
    dedup_matches: int,
    new_documents: int,
):
    """Log ingestion metrics to mlflow or local store."""
    metric = IngestionMetrics(
        source=source,
        fetch_count=fetch_count,
        fetch_errors=fetch_errors,
        dedup_matches=dedup_matches,
        new_documents=new_documents,
        timestamp=datetime.utcnow(),
    )
    # TODO: Send to mlflow
    pass


def log_feature_metric(
    stage: str,
    processed_count: int,
    success_count: int,
    error_count: int,
    avg_latency_seconds: float,
):
    """Log feature computation metrics."""
    metric = FeatureMetrics(
        stage=stage,
        processed_count=processed_count,
        success_count=success_count,
        error_count=error_count,
        avg_latency_seconds=avg_latency_seconds,
        timestamp=datetime.utcnow(),
    )
    # TODO: Send to mlflow
    pass


def log_agent_metric(
    agent_name: str,
    call_count: int,
    success_count: int,
    error_count: int,
    total_cost_usd: float,
    avg_latency_seconds: float,
):
    """Log agent metrics."""
    metric = AgentMetrics(
        agent_name=agent_name,
        call_count=call_count,
        success_count=success_count,
        error_count=error_count,
        total_cost_usd=total_cost_usd,
        avg_latency_seconds=avg_latency_seconds,
        timestamp=datetime.utcnow(),
    )
    # TODO: Send to Langfuse and mlflow
    pass
