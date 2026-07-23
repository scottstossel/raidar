import logging
from typing import Optional
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.ingestion.base import compute_content_hash, compute_url_hash
from src.ingestion.models import Document
from src.db.models import DedupHashORM

logger = logging.getLogger(__name__)


class DedupChecker:
    """Check if a document has been seen before using hash-based dedup."""

    def __init__(self, db_session: Session):
        self.db = db_session

    def _get_existing_hash(self, hash_value: str, hash_type: str) -> Optional[int]:
        """Query dedup table for existing hash. Returns document DB ID if found."""
        query = text(
            f"SELECT document_id FROM dedup_hashes WHERE {hash_type}_hash = :hash LIMIT 1"
        )
        result = self.db.execute(query, {"hash": hash_value}).fetchone()
        return result[0] if result else None

    def is_duplicate(self, doc: Document) -> bool:
        """Check if document is a duplicate. Returns True if already seen."""
        content_hash = compute_content_hash(doc.content)
        url_hash = compute_url_hash(doc.url)

        # Check content hash first (faster)
        if self._get_existing_hash(content_hash, "content"):
            logger.debug(f"Duplicate content detected: {doc.source}/{doc.source_id}")
            return True

        # Check URL hash
        if self._get_existing_hash(url_hash, "url"):
            logger.debug(f"Duplicate URL detected: {doc.source}/{doc.source_id}")
            return True

        return False

    def record_dedup_hashes(self, document_db_id: int, doc: Document):
        """Record the hashes for this document for future dedup checks."""
        content_hash = compute_content_hash(doc.content)
        url_hash = compute_url_hash(doc.url)

        dedup_record = DedupHashORM(
            document_id=document_db_id,
            content_hash=content_hash,
            url_hash=url_hash,
            created_at=datetime.utcnow(),
        )
        self.db.add(dedup_record)
        self.db.commit()
