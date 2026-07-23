"""Ingestion orchestrator: fetch from sources, deduplicate, persist."""

import logging
from typing import List, Dict
from datetime import datetime

from src.ingestion.models import Document
from src.ingestion.arxiv import ArxivAdapter
from src.ingestion.github import GitHubAdapter
from src.ingestion.huggingface import HuggingFaceAdapter
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


def ingest_github(num_repos: int = 50) -> int:
    """Fetch recent repositories and discussions from GitHub."""
    adapter = GitHubAdapter()
    db = get_db_session()
    dedup_checker = DedupChecker(db)

    try:
        # Fetch both repos and discussions
        documents = adapter.fetch_recent_repos(num_repos=num_repos)
        documents.extend(adapter.fetch_trending_discussions(num_discussions=30))
        logger.info(f"Fetched {len(documents)} items from GitHub")

        new_count = 0
        dedup_count = 0

        for doc in documents:
            if dedup_checker.is_duplicate(doc):
                dedup_count += 1
                continue

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
            db.flush()

            dedup_checker.record_dedup_hashes(orm_doc.id, doc)
            new_count += 1

        db.commit()
        logger.info(f"Persisted {new_count} new documents from GitHub, {dedup_count} duplicates")

        log_ingestion_metric(
            source="github",
            fetch_count=len(documents),
            fetch_errors=0,
            dedup_matches=dedup_count,
            new_documents=new_count,
        )

        return new_count

    finally:
        db.close()
        adapter.close()


def ingest_huggingface(num_models: int = 50, num_datasets: int = 30) -> int:
    """Fetch recent models and datasets from Hugging Face Hub."""
    adapter = HuggingFaceAdapter()
    db = get_db_session()
    dedup_checker = DedupChecker(db)

    try:
        documents = adapter.fetch_recent_models(num_models=num_models)
        documents.extend(adapter.fetch_recent_datasets(num_datasets=num_datasets))
        logger.info(f"Fetched {len(documents)} items from Hugging Face")

        new_count = 0
        dedup_count = 0

        for doc in documents:
            if dedup_checker.is_duplicate(doc):
                dedup_count += 1
                continue

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
            db.flush()

            dedup_checker.record_dedup_hashes(orm_doc.id, doc)
            new_count += 1

        db.commit()
        logger.info(
            f"Persisted {new_count} new documents from Hugging Face, {dedup_count} duplicates"
        )

        log_ingestion_metric(
            source="huggingface",
            fetch_count=len(documents),
            fetch_errors=0,
            dedup_matches=dedup_count,
            new_documents=new_count,
        )

        return new_count

    finally:
        db.close()
        adapter.close()


def run_full_ingestion() -> Dict[str, int]:
    """Run ingestion from all sources. Returns dict of new documents per source."""
    results = {}
    results["arxiv"] = ingest_arxiv()
    results["github"] = ingest_github()
    results["huggingface"] = ingest_huggingface()
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = run_full_ingestion()
    total = sum(results.values())
    logger.info(f"Ingestion complete: {total} new documents")
    for source, count in results.items():
        logger.info(f"  {source}: {count} new")
