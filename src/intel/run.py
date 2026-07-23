"""Intel layer runner: orchestrates discovery, analysis, skeptic, synthesis agents."""

import logging
from datetime import datetime
from sqlalchemy import text
from src.db.session import get_db_session
from src.db.models import BriefORM
from src.intel.orchestration import IntelOrchestrator
from src.monitoring.langfuse_tracer import tracer

logger = logging.getLogger(__name__)


def run_intel_pipeline(brief_type: str = "daily") -> dict:
    """
    Run the full intel pipeline on documents with features.

    Args:
        brief_type: "daily" for all documents, or "topic:<topic_name>" for a specific topic

    Returns:
        {
            "brief": {...},
            "processed_count": int,
            "passed_discovery": int,
            "skeptic_escalations": int,
        }
    """
    db = get_db_session()
    orchestrator = IntelOrchestrator()

    try:
        # Fetch documents with features (join documents + document_features)
        if brief_type == "daily":
            query = text(
                """
                SELECT
                    d.id as document_id,
                    d.source,
                    d.title,
                    d.content,
                    df.topic,
                    df.theme,
                    df.trend_score,
                    df.secondary_topics
                FROM documents d
                JOIN document_features df ON d.id = df.document_id
                WHERE d.ingested_at >= CURRENT_DATE
                ORDER BY df.trend_score DESC
                """
            )
        else:
            # Extract topic from brief_type (e.g., "topic:LLMs" -> "LLMs")
            topic = brief_type.replace("topic:", "")
            query = text(
                """
                SELECT
                    d.id as document_id,
                    d.source,
                    d.title,
                    d.content,
                    df.topic,
                    df.theme,
                    df.trend_score,
                    df.secondary_topics
                FROM documents d
                JOIN document_features df ON d.id = df.document_id
                WHERE df.topic = :topic
                ORDER BY df.trend_score DESC
                """
            )
            results = db.execute(query, {"topic": topic}).fetchall()

        if brief_type == "daily":
            results = db.execute(query).fetchall()
        else:
            query = text(
                """
                SELECT
                    d.id as document_id,
                    d.source,
                    d.title,
                    d.content,
                    df.topic,
                    df.theme,
                    df.trend_score,
                    df.secondary_topics
                FROM documents d
                JOIN document_features df ON d.id = df.document_id
                WHERE df.topic = :topic
                ORDER BY df.trend_score DESC
                """
            )
            results = db.execute(query, {"topic": topic}).fetchall()

        # Convert rows to dicts
        documents = [
            {
                "document_id": row[0],
                "source": row[1],
                "title": row[2],
                "content": row[3],
                "topic": row[4],
                "theme": row[5],
                "trend_score": row[6],
                "secondary_topics": (row[7] or "").split(","),
            }
            for row in results
        ]

        logger.info(f"Found {len(documents)} documents for {brief_type} brief")

        if len(documents) == 0:
            logger.warning(f"No documents found for {brief_type}")
            return {
                "brief": {
                    "content": f"No documents available for {brief_type} brief.",
                    "brief_type": brief_type,
                    "success": False,
                },
                "processed_count": 0,
                "passed_discovery": 0,
                "skeptic_escalations": 0,
            }

        # Run intel pipeline
        result = orchestrator.process_documents(documents, brief_type=brief_type)

        logger.info(f"Intel pipeline complete: {result['passed_discovery']} passed discovery")

        # Persist brief to database
        brief = result["brief"]
        if brief["success"]:
            persist_brief(
                db,
                brief_type=brief["brief_type"],
                content=brief["content"],
                themes=brief.get("themes", []),
                document_count=brief.get("document_count", 0),
            )

        return result

    except Exception as e:
        logger.error(f"Intel pipeline failed: {e}")
        return {
            "brief": {
                "content": f"Intel pipeline error: {str(e)}",
                "brief_type": brief_type,
                "success": False,
            },
            "processed_count": 0,
            "passed_discovery": 0,
            "skeptic_escalations": 0,
        }

    finally:
        db.close()
        orchestrator.close()


def persist_brief(
    db_session,
    brief_type: str,
    content: str,
    themes: list,
    document_count: int,
):
    """Persist a generated brief to the database."""
    try:
        brief_orm = BriefORM(
            brief_type=brief_type,
            generated_at=datetime.utcnow(),
            content=content,
            metadata_json={
                "themes": themes,
                "document_count": document_count,
            },
        )
        db_session.add(brief_orm)
        db_session.commit()
        logger.info(f"Persisted {brief_type} brief (ID: {brief_orm.id})")
        return brief_orm.id
    except Exception as e:
        logger.error(f"Failed to persist brief: {e}")
        db_session.rollback()
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_intel_pipeline(brief_type="daily")
    print(f"Intel pipeline complete")
    print(f"Processed: {result['processed_count']}")
    print(f"Passed discovery: {result['passed_discovery']}")
    print(f"Skeptic escalations: {result['skeptic_escalations']}")
