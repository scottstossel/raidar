"""Request and response schemas for the RAIDAR API."""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


class DocumentCard(BaseModel):
    """Minimal document info for list views."""
    document_id: int
    title: str
    source: str
    topic: str
    theme: str
    trend_score: float
    url: str
    fetched_at: datetime


class DocumentDetail(DocumentCard):
    """Full document detail with analysis."""
    content: str
    secondary_topics: List[str] = Field(default_factory=list)
    core_claim: Optional[str] = None
    significance: Optional[str] = None
    limitations: Optional[str] = None
    applicability: Optional[str] = None
    skeptic_flags: List[str] = Field(default_factory=list)


class BriefResponse(BaseModel):
    """Research brief response."""
    brief_id: int
    brief_type: str  # "daily" or "topic:<topic>"
    content: str
    themes: List[str] = Field(default_factory=list)
    document_count: int
    generated_at: datetime
    documents: List[DocumentCard] = Field(default_factory=list)  # Documents included in brief


class TopicsResponse(BaseModel):
    """List of available topics."""
    topics: List[str]
    document_counts: dict  # topic -> count


class HealthResponse(BaseModel):
    """System health status."""
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: datetime
    components: dict  # component_name -> status


class ErrorResponse(BaseModel):
    """Error response."""
    error: str
    detail: Optional[str] = None
    timestamp: datetime
