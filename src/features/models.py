from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any


class DocumentFeatures(BaseModel):
    """Computed features for a document."""

    document_id: int  # Reference to database row
    topic: str  # Primary topic category
    secondary_topics: list[str] = Field(default_factory=list)
    theme: str  # Higher-level theme grouping
    trend_score: float  # 0-1 score for trend/momentum
    embedding_id: str  # ID in Pinecone vector store
    source_contribution: float = 1.0  # Weight for source quality
    computed_at: datetime = Field(default_factory=datetime.utcnow)


class TopicTaggingResult(BaseModel):
    """Result from topic tagging."""

    primary_topic: str
    secondary_topics: list[str] = Field(default_factory=list)
    confidence: float  # 0-1 confidence score


class TrendScoreResult(BaseModel):
    """Result from trend scoring."""

    score: float  # 0-1 score
    velocity: float  # Change in interest over time
    recency_boost: float  # Bonus for recent content
    source_weight: float  # Source credibility multiplier
