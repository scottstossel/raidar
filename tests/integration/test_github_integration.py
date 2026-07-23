"""Integration tests for GitHub adapter (requires network)."""

import pytest
from src.ingestion.github import GitHubAdapter


@pytest.mark.integration
def test_github_adapter_fetch_repos():
    """Test GitHub adapter can fetch repositories (requires network)."""
    adapter = GitHubAdapter()
    try:
        documents = adapter.fetch_recent_repos(num_repos=5)
        assert len(documents) > 0
        assert documents[0].source == "github"
        assert documents[0].source_id.startswith("repo/")
        assert documents[0].url.startswith("https://github.com/")
        assert documents[0].metadata.get("stars") is not None
    finally:
        adapter.close()


@pytest.mark.integration
def test_github_adapter_fetch_discussions():
    """Test GitHub adapter can fetch discussions (requires network)."""
    adapter = GitHubAdapter()
    try:
        documents = adapter.fetch_trending_discussions(num_discussions=5)
        if len(documents) > 0:
            assert documents[0].source in ("github_issue", "github_discussion")
            assert documents[0].url.startswith("https://github.com/")
    finally:
        adapter.close()
