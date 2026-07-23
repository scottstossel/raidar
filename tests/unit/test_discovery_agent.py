"""Unit tests for discovery agent."""

import pytest
from unittest.mock import Mock, patch
from src.intel.agents.discovery import DiscoveryAgent


@patch("src.intel.agents.discovery.Anthropic")
def test_discovery_agent_parses_response(mock_anthropic_class):
    """Test that discovery agent parses relevance response."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    mock_response = Mock()
    mock_response.content = [Mock(text="RELEVANT: yes\nCONFIDENCE: 0.85\nREASONING: Novel method\nPRIORITY: high")]
    mock_client.messages.create.return_value = mock_response

    agent = DiscoveryAgent()
    result = agent.evaluate_relevance(
        document_id=1,
        title="Test Paper",
        source="arxiv",
        topic="LLMs",
        theme="Foundation Models",
        trend_score=0.8,
        content="Test content",
    )

    assert result["relevant"] is True
    assert result["confidence"] == 0.85
    assert result["priority"] == "high"
    assert result["document_id"] == 1


@patch("src.intel.agents.discovery.Anthropic")
def test_discovery_agent_handles_not_relevant(mock_anthropic_class):
    """Test that discovery agent correctly identifies non-relevant documents."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    mock_response = Mock()
    mock_response.content = [Mock(text="RELEVANT: no\nCONFIDENCE: 0.9\nREASONING: Too tangential")]
    mock_client.messages.create.return_value = mock_response

    agent = DiscoveryAgent()
    result = agent.evaluate_relevance(1, "Title", "arxiv", "topic", "theme", 0.5, "content")

    assert result["relevant"] is False
    assert result["confidence"] == 0.9
    assert "priority" not in result  # No priority for non-relevant
