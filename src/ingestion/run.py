"""Ingestion orchestrator: fetch from sources, deduplicate, persist."""

import logging
from typing import List
from datetime import datetime

from src.ingestion.models import Document
from src.ingestion.arxiv import ArxivAdapter
from src.ingestion.dedup import DedupChecker
from src.db.session import get_db_session
from src.db.models import DocumentORM
from src.monitoring.metrics import log_ingestion_metric

logger = logging.getLogger(__name__)


def ingest_arxiv(num_papers: int = 100) -> int:
    """Fetch recent papers from arXiv and persist new ones. Returns count of new documents."""
    adapter = ArxivAdapter()
    db = get_db_session()
    dedup_checker = DedupChecker(db)

    try:
        documents = adapter.fetch_recent(num_papers=num_papers)
        logger.info(f"Fetched {len(documents)} papers from arXiv")

        new_count = 0
        dedup_count = 0

        for doc in documents:
            if dedup_checker.is_duplicate(doc):
                dedup_count += 1
                continue

            # Persist to DB
            orm_doc = DocumentORM(
                source=doc.source,
                source_id=doc.source_id,
                title=doc.title,
                content=doc.content,
                url=doc.url,
                metadata_json=doc.metadata,
                fetched_at=doc.fetched_at,
                ingested_at=datetime.utcnow(),
            )
            db.add(orm_doc)
            db.flush()  # Get the ID

            # Record hashes for future dedup
            dedup_checker.record_dedup_hashes(orm_doc.id, doc)
            new_count += 1

        db.commit()
        logger.info(f"Persisted {new_count} new documents from arXiv, {dedup_count} duplicates")

        # Log metrics
        log_ingestion_metric(
            source="arxiv",
            fetch_count=len(documents),
            fetch_errors=0,  # Would be higher if we had fetch failures
            dedup_matches=dedup_count,
            new_documents=new_count,
        )

        return new_count

    finally:
        db.close()
        adapter.close()


def run_full_ingestion() -> int:
    """Run ingestion from all sources. Returns total new document count."""
    # TODO: Add GitHub and HuggingFace adapters here
    total_new = 0
    total_new += ingest_arxiv()
    return total_new


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = run_full_ingestion()
    print(f"Ingestion complete: {count} new documents")
