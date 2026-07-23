"""Unit tests for analysis agent."""

import pytest
from unittest.mock import Mock, patch
from src.intel.agents.analysis import AnalysisAgent


@patch("src.intel.agents.analysis.Anthropic")
def test_analysis_agent_extracts_json(mock_anthropic_class):
    """Test that analysis agent parses JSON response."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    json_response = """{
        "core_claim": "Novel transformer architecture with 50% faster inference",
        "significance": "Enables deployment on edge devices",
        "limitations": "Only tested on image tasks",
        "applicability": "immediate",
        "key_metrics": "50% faster, 90% accuracy parity"
    }"""

    mock_response = Mock()
    mock_response.content = [Mock(text=json_response)]
    mock_client.messages.create.return_value = mock_response

    agent = AnalysisAgent()
    result = agent.extract_signal(1, "Title", "topic", "content")

    assert result["success"] is True
    assert "faster" in result["core_claim"].lower()
    assert result["applicability"] == "immediate"
    assert result["document_id"] == 1


@patch("src.intel.agents.analysis.Anthropic")
def test_analysis_agent_handles_malformed_json(mock_anthropic_class):
    """Test that analysis agent gracefully handles malformed JSON."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    mock_response = Mock()
    mock_response.content = [Mock(text="This is not JSON")]
    mock_client.messages.create.return_value = mock_response

    agent = AnalysisAgent()
    result = agent.extract_signal(1, "Title", "topic", "content")

    assert result["success"] is False
    assert result["core_claim"] == ""
    assert result["applicability"] == "unknown"
