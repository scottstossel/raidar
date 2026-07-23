from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from src.config import settings
from src.db.models import Base

engine = create_engine(settings.database_url, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db_session() -> Session:
    """Get a database session for use in a context manager."""
    return SessionLocal()


def init_db():
    """Create all tables if they don't exist."""
    Base.metadata.create_all(bind=engine)
