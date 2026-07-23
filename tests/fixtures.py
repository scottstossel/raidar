"""Test fixtures for RAIDAR."""

import pytest
from datetime import datetime
from src.ingestion.models import Document
from src.db.session import SessionLocal, init_db
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine


@pytest.fixture
def test_db():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    from src.db.models import Base
    Base.metadata.create_all(bind=engine)

    from sqlalchemy.orm import sessionmaker
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    yield TestingSessionLocal()

    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def sample_document() -> Document:
    """Create a sample document for testing."""
    return Document(
        source="arxiv",
        source_id="2024.01234",
        title="A Breakthrough in AI",
        content="This paper presents a novel approach to transformers.",
        url="https://arxiv.org/abs/2024.01234",
        metadata={"authors": ["Alice", "Bob"]},
        fetched_at=datetime.utcnow(),
    )
