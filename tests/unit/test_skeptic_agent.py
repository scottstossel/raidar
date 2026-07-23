"""Unit tests for skeptic agent."""

import pytest
from unittest.mock import Mock, patch
from src.intel.agents.skeptic import SkepticAgent


@patch("src.intel.agents.skeptic.Anthropic")
def test_skeptic_agent_parses_checklist(mock_anthropic_class):
    """Test that skeptic agent parses verification checklist."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    response_text = """
SOURCE_MATCH: ok
BENCHMARK_COMPARISON: concern/unfair comparison to older baselines
PUBLICATION_STATUS: peer-reviewed
ADOPTION_SIGNALS: ok
REPRODUCIBILITY: ok
OVERALL_FLAGS: benchmark_unfair
"""

    mock_response = Mock()
    mock_response.content = [Mock(text=response_text)]
    mock_client.messages.create.return_value = mock_response

    agent = SkepticAgent()
    result = agent.verify_claims(1, "Title", "arxiv", "claim", "content")

    assert result["source_match"] == "ok"
    assert "concern" in result["benchmark_comparison"]
    assert result["publication_status"] == "peer-reviewed"
    assert "benchmark_unfair" in result["flags"]


@patch("src.intel.agents.skeptic.Anthropic")
def test_skeptic_agent_escalation_needed(mock_anthropic_class):
    """Test that skeptic agent flags escalation when multiple issues found."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    response_text = """
SOURCE_MATCH: concern/misquoted
BENCHMARK_COMPARISON: concern/unfair
PUBLICATION_STATUS: preprint
ADOPTION_SIGNALS: ok
REPRODUCIBILITY: ok
OVERALL_FLAGS: source_mismatch, benchmark_unfair, preprint_only
"""

    mock_response = Mock()
    mock_response.content = [Mock(text=response_text)]
    mock_client.messages.create.return_value = mock_response

    agent = SkepticAgent()
    result = agent.verify_claims(1, "Title", "arxiv", "claim", "content")

    assert result["escalation_needed"] is True
    assert len(result["flags"]) >= 2
