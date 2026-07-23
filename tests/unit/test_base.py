"""Unit tests for ingestion base utilities."""

import pytest
from src.ingestion.base import normalize_url, compute_content_hash, compute_url_hash


def test_normalize_url():
    """Test URL normalization."""
    url1 = "https://arxiv.org/abs/2024.01234#section1"
    url2 = "https://arxiv.org/abs/2024.01234"
    assert normalize_url(url1) == normalize_url(url2)


def test_compute_content_hash():
    """Test content hashing is consistent."""
    content = "This is a test document."
    hash1 = compute_content_hash(content)
    hash2 = compute_content_hash(content)
    assert hash1 == hash2
    assert len(hash1) == 64  # SHA256 hex is 64 chars


def test_compute_url_hash():
    """Test URL hashing."""
    url = "https://arxiv.org/abs/2024.01234"
    hash_val = compute_url_hash(url)
    assert len(hash_val) == 64
