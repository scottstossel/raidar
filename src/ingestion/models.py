from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any

class Document(BaseModel):
    """Normalized document from ingestion, before feature computation."""

    source: str  # 'arxiv', 'github', 'huggingface'
    source_id: str  # ID within the source system (arxiv ID, GitHub URL, etc.)
    title: str
    content: str  # Abstract, README, description, or full text
    url: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    fetched_at: datetime

    # Filled in by ingestion pipeline
    ingested_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Database row ID (set after persistence)
    db_id: Optional[int] = None


class DocumentWithFeatures(Document):
    """Document extended with computed features."""

    topic: Optional[str] = None
    theme: Optional[str] = None
    trend_score: Optional[float] = None
    embedding_id: Optional[str] = None  # Pinecone vector ID

    # Agent outputs (added during intel layer)
    discovery_relevance: Optional[float] = None  # 0-1 relevance score
    analysis_signal: Optional[Dict[str, Any]] = None  # What it claims, why it matters
    skeptic_flags: Optional[list[str]] = None  # Issues found (e.g., 'unverified_claim', 'novelty_only')
