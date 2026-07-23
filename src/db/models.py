from sqlalchemy import Column, Integer, String, Text, DateTime, Float, JSON, ForeignKey, Index, func
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class DocumentORM(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)  # 'arxiv', 'github', 'huggingface'
    source_id = Column(String(255), nullable=False)
    title = Column(String(500), nullable=False)
    content = Column(Text, nullable=False)
    url = Column(String(2048), nullable=False)
    metadata_json = Column(JSON, nullable=True)
    fetched_at = Column(DateTime, nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_documents_source_source_id", "source", "source_id", unique=True),
        Index("ix_documents_ingested_at", "ingested_at"),
    )


class DedupHashORM(Base):
    __tablename__ = "dedup_hashes"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False)
    content_hash = Column(String(64), nullable=False)  # SHA256 hex
    url_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_dedup_hashes_content_hash", "content_hash"),
        Index("ix_dedup_hashes_url_hash", "url_hash"),
    )


class BriefORM(Base):
    __tablename__ = "briefs"

    id = Column(Integer, primary_key=True)
    brief_type = Column(String(50), nullable=False)  # 'daily' or 'topic:<topic_name>'
    generated_at = Column(DateTime, default=datetime.utcnow)
    content = Column(Text, nullable=False)  # Narrative brief text
    metadata_json = Column(JSON, nullable=True)  # e.g., {topic: 'agents', num_documents: 42}
    ingestion_time_seconds = Column(Float, nullable=True)


class DocumentFeaturesORM(Base):
    __tablename__ = "document_features"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey("documents.id"), nullable=False)
    topic = Column(String(100), nullable=False)
    secondary_topics = Column(String(255), nullable=True)  # Comma-separated
    theme = Column(String(100), nullable=False)
    trend_score = Column(Float, nullable=False)
    embedding_id = Column(String(255), nullable=True)  # Pinecone vector ID
    source_contribution = Column(Float, default=1.0)
    computed_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("ix_document_features_document_id", "document_id"),)


class AuditLogORM(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True)
    stage = Column(String(100), nullable=False)  # 'ingestion', 'features', 'intel', etc.
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)
    metadata_json = Column(JSON, nullable=True)
