"""Feature computation orchestrator: topic tagging, embeddings, trends, themes."""

import logging
from datetime import datetime
from sqlalchemy import text
from src.db.session import get_db_session
from src.db.models import DocumentORM
from src.features.topic_tagging import TopicTagger
from src.features.embeddings import EmbeddingService
from src.features.trend_scoring import TrendScorer
from src.features.themes import ThemeAssigner
from src.monitoring.metrics import log_feature_metric

logger = logging.getLogger(__name__)


def compute_features_for_new_documents(batch_size: int = 100) -> int:
    """
    Compute features for documents that don't have them yet.
    Returns count of documents processed.
    """
    db = get_db_session()
    tagger = TopicTagger()
    embedder = EmbeddingService()
    scorer = TrendScorer(db)
    themer = ThemeAssigner()

    try:
        # Find documents without features
        query = text(
            "SELECT id, source, title, content FROM documents "
            "WHERE id NOT IN (SELECT DISTINCT document_id FROM document_features) "
            "ORDER BY ingested_at DESC LIMIT :limit"
        )
        results = db.execute(query, {"limit": batch_size}).fetchall()
        logger.info(f"Found {len(results)} documents needing feature computation")

        processed = 0
        errors = 0

        for doc_id, source, title, content in results:
            try:
                # 1. Topic tagging
                topic_result = tagger.tag_document(title, content)

                # 2. Embeddings
                embedding_id = embedder.embed_document(
                    content,
                    str(doc_id),
                    metadata={
                        "document_id": doc_id,
                        "source": source,
                        "topic": topic_result.primary_topic,
                    },
                )

                # 3. Trend scoring
                trend_result = scorer.score_document(doc_id, source)

                # 4. Theme assignment
                theme = themer.assign_theme(title, content, topic_result.primary_topic)

                # Store features in database
                insert_query = text(
                    "INSERT INTO document_features "
                    "(document_id, topic, secondary_topics, theme, trend_score, "
                    "embedding_id, source_contribution, computed_at) "
                    "VALUES (:doc_id, :topic, :sec_topics, :theme, :score, "
                    ":embedding_id, :source_weight, :now)"
                )
                db.execute(
                    insert_query,
                    {
                        "doc_id": doc_id,
                        "topic": topic_result.primary_topic,
                        "sec_topics": ",".join(topic_result.secondary_topics),
                        "theme": theme,
                        "score": trend_result.score,
                        "embedding_id": embedding_id,
                        "source_weight": trend_result.source_weight,
                        "now": datetime.utcnow(),
                    },
                )
                db.commit()
                processed += 1

            except Exception as e:
                logger.error(f"Failed to compute features for document {doc_id}: {e}")
                errors += 1
                continue

        logger.info(f"Computed features for {processed} documents ({errors} errors)")

        # Log metrics
        log_feature_metric(
            stage="full_pipeline",
            processed_count=len(results),
            success_count=processed,
            error_count=errors,
            avg_latency_seconds=0.0,  # TODO: track timing
        )

        return processed

    finally:
        db.close()
        embedder.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = compute_features_for_new_documents()
    print(f"Feature computation complete: {count} documents processed")
