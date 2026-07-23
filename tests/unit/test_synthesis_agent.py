"""Unit tests for synthesis agent."""

import pytest
from unittest.mock import Mock, patch
from src.intel.agents.synthesis import SynthesisAgent


@patch("src.intel.agents.synthesis.Anthropic")
def test_synthesis_agent_generates_brief(mock_anthropic_class):
    """Test that synthesis agent generates a brief."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    brief_text = """# Daily AI Research Brief
**Date:** 2024-01-15

## 🔥 Highlights
Today brings major advances in efficient inference and multimodal models.

## Themes & Discoveries
### Efficient Inference
Recent work on model quantization and distillation shows 50% speedups...

### Multimodal Models
New vision-language models demonstrate improved zero-shot performance...
"""

    mock_response = Mock()
    mock_response.content = [Mock(text=brief_text)]
    mock_client.messages.create.return_value = mock_response

    agent = SynthesisAgent()
    result = agent.generate_brief(
        [
            {
                "title": "Paper 1",
                "topic": "LLMs",
                "core_claim": "New architecture",
                "significance": "Fast inference",
                "flags": [],
            },
        ],
        brief_type="daily",
    )

    assert result["success"] is True
    assert "AI Research Brief" in result["content"]
    assert result["document_count"] == 1
    assert len(result["themes"]) > 0


def test_synthesis_agent_handles_empty_documents():
    """Test that synthesis agent handles empty document list gracefully."""
    agent = SynthesisAgent()
    result = agent.generate_brief([], brief_type="daily")

    assert result["success"] is False
    assert "No documents" in result["content"]
    assert result["document_count"] == 0
