"""Unit tests for dedup logic."""

import pytest
from datetime import datetime
from src.ingestion.models import Document
from src.ingestion.dedup import DedupChecker
from src.db.models import DocumentORM, DedupHashORM
from src.db.session import SessionLocal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


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


def test_dedup_detects_duplicate_content(test_db):
    """Test that dedup detects duplicate content."""
    doc1 = Document(
        source="arxiv",
        source_id="2024.00001",
        title="Test Paper",
        content="This is the abstract of a paper.",
        url="https://arxiv.org/abs/2024.00001",
        fetched_at=datetime.utcnow(),
    )

    doc2 = Document(
        source="github",
        source_id="repo/issue123",
        title="Test Issue",
        content="This is the abstract of a paper.",  # Same content
        url="https://github.com/repo/issue/123",
        fetched_at=datetime.utcnow(),
    )

    dedup = DedupChecker(test_db)

    # First document should not be a duplicate
    assert not dedup.is_duplicate(doc1)

    # Persist it
    orm1 = DocumentORM(
        source=doc1.source,
        source_id=doc1.source_id,
        title=doc1.title,
        content=doc1.content,
        url=doc1.url,
        fetched_at=doc1.fetched_at,
    )
    test_db.add(orm1)
    test_db.flush()
    dedup.record_dedup_hashes(orm1.id, doc1)

    # Second document with same content should be a duplicate
    assert dedup.is_duplicate(doc2)


def test_dedup_detects_duplicate_url(test_db):
    """Test that dedup detects duplicate URLs."""
    doc1 = Document(
        source="arxiv",
        source_id="2024.00001",
        title="Test Paper",
        content="First version of abstract.",
        url="https://arxiv.org/abs/2024.00001",
        fetched_at=datetime.utcnow(),
    )

    doc2 = Document(
        source="arxiv",
        source_id="2024.00001",
        title="Test Paper Updated",
        content="Updated abstract.",
        url="https://arxiv.org/abs/2024.00001",  # Same URL
        fetched_at=datetime.utcnow(),
    )

    dedup = DedupChecker(test_db)

    # Persist first
    orm1 = DocumentORM(
        source=doc1.source,
        source_id=doc1.source_id,
        title=doc1.title,
        content=doc1.content,
        url=doc1.url,
        fetched_at=doc1.fetched_at,
    )
    test_db.add(orm1)
    test_db.flush()
    dedup.record_dedup_hashes(orm1.id, doc1)

    # Second with same URL should be a duplicate
    assert dedup.is_duplicate(doc2)
