"""FastAPI routes for RAIDAR."""

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from src.api.schemas import (
    BriefResponse, DocumentCard, DocumentDetail, TopicsResponse, HealthResponse, ErrorResponse
)
from src.db.session import get_db_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["raidar"])


@router.get("/health", response_model=HealthResponse)
def health():
    """Check system health."""
    try:
        db = get_db_session()
        db.execute(text("SELECT 1"))
        db.close()
        return HealthResponse(
            status="healthy",
            timestamp=datetime.utcnow(),
            components={"database": "ok"},
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="degraded",
            timestamp=datetime.utcnow(),
            components={"database": "error"},
        )


@router.get("/emerging", response_model=BriefResponse)
def get_daily_brief():
    """Get today's daily AI research brief."""
    db = get_db_session()
    try:
        # Fetch latest daily brief
        query = text(
            """
            SELECT id, brief_type, content, metadata_json
            FROM briefs
            WHERE brief_type = 'daily'
            ORDER BY generated_at DESC
            LIMIT 1
            """
        )
        result = db.execute(query).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="No brief available yet")

        brief_id, brief_type, content, metadata_json = result
        themes = metadata_json.get("themes", []) if metadata_json else []
        doc_count = metadata_json.get("document_count", 0) if metadata_json else 0

        return BriefResponse(
            brief_id=brief_id,
            brief_type=brief_type,
            content=content,
            themes=themes,
            document_count=doc_count,
            generated_at=datetime.utcnow(),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch brief: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch brief")
    finally:
        db.close()


@router.get("/briefs/topics/{topic}", response_model=BriefResponse)
def get_topic_brief(topic: str):
    """Get brief for a specific topic."""
    db = get_db_session()
    try:
        brief_type = f"topic:{topic}"

        query = text(
            """
            SELECT id, brief_type, content, metadata_json
            FROM briefs
            WHERE brief_type = :brief_type
            ORDER BY generated_at DESC
            LIMIT 1
            """
        )
        result = db.execute(query, {"brief_type": brief_type}).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail=f"No brief available for topic: {topic}")

        brief_id, brief_type, content, metadata_json = result
        themes = metadata_json.get("themes", []) if metadata_json else []
        doc_count = metadata_json.get("document_count", 0) if metadata_json else 0

        return BriefResponse(
            brief_id=brief_id,
            brief_type=brief_type,
            content=content,
            themes=themes,
            document_count=doc_count,
            generated_at=datetime.utcnow(),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch topic brief: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch topic brief")
    finally:
        db.close()


@router.get("/topics", response_model=TopicsResponse)
def list_topics():
    """List available topics and document counts."""
    db = get_db_session()
    try:
        query = text(
            """
            SELECT topic, COUNT(*) as count
            FROM document_features
            GROUP BY topic
            ORDER BY count DESC
            """
        )
        results = db.execute(query).fetchall()

        topics = [row[0] for row in results]
        counts = {row[0]: row[1] for row in results}

        return TopicsResponse(topics=topics, document_counts=counts)

    except Exception as e:
        logger.error(f"Failed to list topics: {e}")
        raise HTTPException(status_code=500, detail="Failed to list topics")
    finally:
        db.close()


@router.get("/docs/{doc_id}", response_model=DocumentDetail)
def get_document(doc_id: int):
    """Get full document details with analysis."""
    db = get_db_session()
    try:
        query = text(
            """
            SELECT
                d.id, d.source, d.title, d.content, d.url, d.fetched_at,
                df.topic, df.secondary_topics, df.theme, df.trend_score
            FROM documents d
            LEFT JOIN document_features df ON d.id = df.document_id
            WHERE d.id = :doc_id
            """
        )
        result = db.execute(query, {"doc_id": doc_id}).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Document not found")

        (doc_id, source, title, content, url, fetched_at,
         topic, secondary_topics_str, theme, trend_score) = result

        secondary_topics = (
            [t.strip() for t in secondary_topics_str.split(",")]
            if secondary_topics_str
            else []
        )

        return DocumentDetail(
            document_id=doc_id,
            title=title,
            source=source,
            topic=topic or "Unknown",
            theme=theme or "Unknown",
            trend_score=trend_score or 0.0,
            url=url,
            fetched_at=fetched_at,
            content=content,
            secondary_topics=secondary_topics,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch document: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch document")
    finally:
        db.close()
