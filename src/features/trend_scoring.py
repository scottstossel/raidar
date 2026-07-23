import logging
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
from src.features.models import TrendScoreResult

logger = logging.getLogger(__name__)

# Source credibility weights (higher = more credible)
SOURCE_WEIGHTS = {
    "arxiv": 1.2,  # Peer-reviewed scientific papers
    "github": 1.0,  # Code and discussions
    "huggingface": 0.9,  # Pre-trained models
    "github_issue": 0.8,  # Community discussions
    "github_discussion": 0.8,
    "huggingface_dataset": 0.9,
}


class TrendScorer:
    """Compute trend scores using statistical methods."""

    def __init__(self, db_session: Session):
        self.db = db_session

    def score_document(self, document_id: int, source: str) -> TrendScoreResult:
        """
        Compute trend score for a document.

        Score considers:
        - Recency (newer = higher score)
        - Velocity (how quickly similar content appeared)
        - Source credibility
        """
        # Get document metadata
        doc_query = text(
            "SELECT fetched_at FROM documents WHERE id = :doc_id"
        )
        result = self.db.execute(doc_query, {"doc_id": document_id}).fetchone()
        if not result:
            return TrendScoreResult(score=0.0, velocity=0.0, recency_boost=0.0, source_weight=1.0)

        fetched_at = result[0]

        # Recency boost: documents from last 7 days get higher scores
        days_old = (datetime.utcnow() - fetched_at).days
        recency_boost = max(0.0, 1.0 - (days_old / 30.0))  # Linear decay over 30 days

        # Velocity: count how many similar documents appeared in last 7 days
        velocity = self._compute_velocity(fetched_at)

        # Source weight
        source_weight = SOURCE_WEIGHTS.get(source, 1.0)

        # Combine into final score (0-1)
        base_score = 0.5 + (recency_boost * 0.3) + (velocity * 0.2)
        final_score = min(1.0, max(0.0, base_score * source_weight))

        return TrendScoreResult(
            score=final_score,
            velocity=velocity,
            recency_boost=recency_boost,
            source_weight=source_weight,
        )

    def _compute_velocity(self, reference_date: datetime) -> float:
        """
        Compute velocity: rate of similar content appearing.
        Returns 0-1 score (higher = more velocity).
        """
        # Count documents from same 7-day window
        week_ago = reference_date - timedelta(days=7)
        count_query = text(
            "SELECT COUNT(*) FROM documents WHERE fetched_at BETWEEN :week_ago AND :now"
        )
        result = self.db.execute(
            count_query,
            {"week_ago": week_ago, "now": reference_date},
        ).fetchone()
        count = result[0] if result else 0

        # Normalize: more than 50 documents in a week = high velocity
        velocity = min(1.0, count / 50.0)
        return velocity

    def score_batch(self, document_ids: list[int], sources: list[str]) -> list[TrendScoreResult]:
        """Score multiple documents."""
        return [
            self.score_document(doc_id, source)
            for doc_id, source in zip(document_ids, sources)
        ]
