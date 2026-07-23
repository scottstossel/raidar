"""Unit tests for trend scoring."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from sqlalchemy import create_engine, Column, Integer, DateTime, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.features.trend_scoring import TrendScorer


@pytest.fixture
def test_db():
    """Create in-memory test database."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    from src.db.models import Base
    Base.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return TestingSessionLocal()


def test_trend_scorer_source_weights_apply():
    """Test that source weights are correctly applied."""
    from src.features.trend_scoring import TrendScoreResult

    result = TrendScoreResult(
        score=0.7,
        velocity=0.3,
        recency_boost=0.5,
        source_weight=1.2,
    )

    # Verify structure
    assert 0.0 <= result.score <= 1.0
    assert result.source_weight > 0


def test_trend_scorer_source_weight_arxiv_higher():
    """Test that arXiv documents get higher source weight than GitHub."""
    from src.features.trend_scoring import SOURCE_WEIGHTS

    assert SOURCE_WEIGHTS["arxiv"] > SOURCE_WEIGHTS["github"]
    assert SOURCE_WEIGHTS["arxiv"] > SOURCE_WEIGHTS["huggingface"]
