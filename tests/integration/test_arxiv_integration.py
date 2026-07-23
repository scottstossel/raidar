"""Integration tests for arXiv adapter (requires network)."""

import pytest
from datetime import datetime
from src.ingestion.arxiv import ArxivAdapter


@pytest.mark.integration
def test_arxiv_adapter_fetch():
    """Test arXiv adapter can fetch papers (requires network)."""
    adapter = ArxivAdapter(email="test@example.com")
    try:
        documents = adapter.fetch_recent(num_papers=5)
        assert len(documents) > 0
        assert documents[0].source == "arxiv"
        assert documents[0].source_id
        assert documents[0].title
        assert documents[0].url.startswith("https://arxiv.org/abs/")
    finally:
        adapter.close()


@pytest.mark.integration
def test_arxiv_adapter_rate_limit():
    """Test that arXiv adapter respects rate limits."""
    adapter = ArxivAdapter(email="test@example.com")
    try:
        # Make two requests quickly; adapter should rate-limit
        import time
        start = time.time()
        adapter._search_category("cs.AI", 1, 5)
        adapter._search_category("cs.LG", 1, 5)
        elapsed = time.time() - start

        # Should take at least 3 seconds (rate limit is 1 request per 3 seconds)
        assert elapsed >= 3.0
    finally:
        adapter.close()
