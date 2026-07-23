"""Unit tests for API schemas."""

from datetime import datetime
from src.api.schemas import DocumentCard, BriefResponse, TopicsResponse, HealthResponse


def test_document_card_schema():
    """Test DocumentCard schema."""
    doc = DocumentCard(
        document_id=1,
        title="Test Paper",
        source="arxiv",
        topic="LLMs",
        theme="Foundation Models",
        trend_score=0.8,
        url="https://arxiv.org/abs/2024.00001",
        fetched_at=datetime.utcnow(),
    )

    assert doc.document_id == 1
    assert doc.title == "Test Paper"
    assert doc.source == "arxiv"


def test_brief_response_schema():
    """Test BriefResponse schema."""
    brief = BriefResponse(
        brief_id=1,
        brief_type="daily",
        content="Today's AI research brief...",
        themes=["LLMs", "Vision"],
        document_count=42,
        generated_at=datetime.utcnow(),
    )

    assert brief.brief_id == 1
    assert brief.brief_type == "daily"
    assert len(brief.themes) == 2
    assert brief.document_count == 42


def test_topics_response_schema():
    """Test TopicsResponse schema."""
    topics = TopicsResponse(
        topics=["LLMs", "Vision", "RL"],
        document_counts={"LLMs": 50, "Vision": 30, "RL": 20},
    )

    assert len(topics.topics) == 3
    assert topics.document_counts["LLMs"] == 50


def test_health_response_schema():
    """Test HealthResponse schema."""
    health = HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        components={"database": "ok", "cache": "ok"},
    )

    assert health.status == "healthy"
    assert health.components["database"] == "ok"
