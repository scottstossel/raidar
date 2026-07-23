"""Integration tests for Hugging Face adapter (requires network)."""

import pytest
from src.ingestion.huggingface import HuggingFaceAdapter


@pytest.mark.integration
def test_huggingface_adapter_fetch_models():
    """Test Hugging Face adapter can fetch models (requires network)."""
    adapter = HuggingFaceAdapter()
    try:
        documents = adapter.fetch_recent_models(num_models=5)
        assert len(documents) > 0
        assert documents[0].source == "huggingface"
        assert documents[0].source_id.startswith("model/")
        assert documents[0].url.startswith("https://huggingface.co/")
    finally:
        adapter.close()


@pytest.mark.integration
def test_huggingface_adapter_fetch_datasets():
    """Test Hugging Face adapter can fetch datasets (requires network)."""
    adapter = HuggingFaceAdapter()
    try:
        documents = adapter.fetch_recent_datasets(num_datasets=5)
        assert len(documents) > 0
        assert documents[0].source == "huggingface_dataset"
        assert documents[0].source_id.startswith("dataset/")
        assert documents[0].url.startswith("https://huggingface.co/datasets/")
    finally:
        adapter.close()
