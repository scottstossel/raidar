import logging
import hashlib
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def normalize_url(url: str) -> str:
    """Normalize URL for dedup (remove fragments, query params that don't matter)."""
    parsed = urlparse(url)
    # Reconstruct without fragment
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if parsed.query:
        normalized += f"?{parsed.query}"
    return normalized.lower()


def compute_content_hash(content: str) -> str:
    """Compute SHA256 hash of normalized content."""
    normalized_content = content.strip().lower()
    return hashlib.sha256(normalized_content.encode()).hexdigest()


def compute_url_hash(url: str) -> str:
    """Compute SHA256 hash of normalized URL."""
    normalized = normalize_url(url)
    return hashlib.sha256(normalized.encode()).hexdigest()


class FetchError(Exception):
    """Raised when fetching from a source fails."""
    pass


def log_fetch_attempt(source: str, identifier: str, success: bool, error: Optional[str] = None):
    """Log fetch attempt for observability."""
    if success:
        logger.info(f"Fetched {source}/{identifier}")
    else:
        logger.warning(f"Failed to fetch {source}/{identifier}: {error}")
