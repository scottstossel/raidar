"""Unit tests for topic tagging."""

import pytest
from unittest.mock import Mock, patch
from src.features.topic_tagging import TopicTagger


@patch("src.features.topic_tagging.Anthropic")
def test_topic_tagger_parses_response(mock_anthropic_class):
    """Test that topic tagger parses LLM response correctly."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    # Mock LLM response
    mock_response = Mock()
    mock_response.content = [Mock(text="PRIMARY_TOPIC: Large Language Models\nCONFIDENCE: 0.9\nSECONDARY_TOPICS: fine-tuning, alignment")]
    mock_client.messages.create.return_value = mock_response

    tagger = TopicTagger()
    result = tagger.tag_document(
        "Fine-tuning LLMs",
        "This paper discusses techniques for fine-tuning large language models."
    )

    assert result.primary_topic == "Large Language Models"
    assert result.confidence == 0.9
    assert "fine-tuning" in result.secondary_topics
    assert "alignment" in result.secondary_topics


@patch("src.features.topic_tagging.Anthropic")
def test_topic_tagger_handles_malformed_response(mock_anthropic_class):
    """Test that topic tagger handles malformed LLM responses gracefully."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    # Mock malformed response
    mock_response = Mock()
    mock_response.content = [Mock(text="garbage response")]
    mock_client.messages.create.return_value = mock_response

    tagger = TopicTagger()
    result = tagger.tag_document("Title", "Content")

    assert result.primary_topic == "Unknown"
    assert result.confidence == 0.5  # Default when parsing fails


@patch("src.features.topic_tagging.Anthropic")
def test_topic_tagger_clamps_confidence(mock_anthropic_class):
    """Test that confidence is clamped to [0, 1]."""
    mock_client = Mock()
    mock_anthropic_class.return_value = mock_client

    mock_response = Mock()
    mock_response.content = [Mock(text="PRIMARY_TOPIC: AI Safety & Ethics\nCONFIDENCE: 1.5")]
    mock_client.messages.create.return_value = mock_response

    tagger = TopicTagger()
    result = tagger.tag_document("Title", "Content")

    assert 0.0 <= result.confidence <= 1.0
    assert result.confidence == 1.0
