"""Unit tests for ingestion runner."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from src.ingestion.run import ingest_arxiv, ingest_github, ingest_huggingface
from src.ingestion.models import Document


@pytest.fixture
def sample_docs():
    """Create sample documents for testing."""
    return [
        Document(
            source="arxiv",
            source_id="2024.00001",
            title="Test Paper 1",
            content="Abstract of test paper 1",
            url="https://arxiv.org/abs/2024.00001",
            fetched_at=datetime.utcnow(),
        ),
        Document(
            source="arxiv",
            source_id="2024.00002",
            title="Test Paper 2",
            content="Abstract of test paper 2",
            url="https://arxiv.org/abs/2024.00002",
            fetched_at=datetime.utcnow(),
        ),
    ]


@patch("src.ingestion.run.ArxivAdapter")
@patch("src.ingestion.run.get_db_session")
def test_ingest_arxiv_persists_new_documents(mock_db, mock_adapter_class, sample_docs):
    """Test that ingest_arxiv persists new documents."""
    # Mock adapter
    mock_adapter = Mock()
    mock_adapter_class.return_value = mock_adapter
    mock_adapter.fetch_recent.return_value = sample_docs

    # Mock DB session and ORM
    mock_session = MagicMock()
    mock_db.return_value = mock_session
    mock_session.add = Mock()
    mock_session.flush = Mock()
    mock_session.commit = Mock()

    # Mock DedupChecker
    with patch("src.ingestion.run.DedupChecker") as mock_dedup:
        mock_dedup_instance = Mock()
        mock_dedup.return_value = mock_dedup_instance
        mock_dedup_instance.is_duplicate.return_value = False
        mock_dedup_instance.record_dedup_hashes = Mock()

        # Run ingestion
        count = ingest_arxiv(num_papers=10)

        # Verify persistence
        assert count == 2
        assert mock_session.add.call_count == 2
        assert mock_session.commit.called


@patch("src.ingestion.run.ArxivAdapter")
@patch("src.ingestion.run.get_db_session")
def test_ingest_arxiv_filters_duplicates(mock_db, mock_adapter_class, sample_docs):
    """Test that ingest_arxiv filters out duplicates."""
    mock_adapter = Mock()
    mock_adapter_class.return_value = mock_adapter
    mock_adapter.fetch_recent.return_value = sample_docs * 2  # Duplicate docs

    mock_session = MagicMock()
    mock_db.return_value = mock_session

    with patch("src.ingestion.run.DedupChecker") as mock_dedup:
        mock_dedup_instance = Mock()
        mock_dedup.return_value = mock_dedup_instance
        # Second set of docs are duplicates
        mock_dedup_instance.is_duplicate.side_effect = [False, False, True, True]
        mock_dedup_instance.record_dedup_hashes = Mock()

        count = ingest_arxiv(num_papers=20)

        assert count == 2  # Only first 2 should be persisted
        assert mock_session.add.call_count == 2
