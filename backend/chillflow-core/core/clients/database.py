"""
Database client for ChillFlow with connection management and utilities.

This module provides a database client with connection pooling, session management,
and common database operations for the ChillFlow system.
"""

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from core.models import Base
from core.settings import settings
from core.utils.logging import get_logger

logger = get_logger("database-client")


class DatabaseClient:
    """Database client with connection management and utilities."""

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database client.

        Args:
            database_url: Database connection URL. Uses settings if not provided.
        """
        self.database_url = database_url or settings.DATABASE_URL
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None
        self._setup_engine()

    def _setup_engine(self) -> None:
        """Setup SQLAlchemy engine with connection pooling."""
        try:
            self.engine = create_engine(
                self.database_url,
                poolclass=QueuePool,
                pool_size=20,
                max_overflow=30,
                pool_timeout=30,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,  # Set to True for SQL query logging
            )

            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )

            logger.info(
                "Database engine initialized",
                url=(
                    self.database_url.split("@")[1]
                    if "@" in self.database_url
                    else "configured"
                ),
            )

        except Exception as e:
            logger.error("Failed to initialize database engine", error=str(e))
            raise

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get database session with automatic cleanup.

        Yields:
            SQLAlchemy session

        Example:
            with db_client.get_session() as session:
                trip = session.query(CompleteTrip).first()
        """
        if not self.SessionLocal:
            raise RuntimeError("Database client not initialized")

        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("Database session error", error=str(e))
            raise
        finally:
            session.close()

    def create_tables(self) -> None:
        """Create all database tables."""
        if not self.engine:
            raise RuntimeError("Database engine not initialized")

        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error("Failed to create database tables", error=str(e))
            raise

    def drop_tables(self) -> None:
        """Drop all database tables."""
        if not self.engine:
            raise RuntimeError("Database engine not initialized")

        try:
            Base.metadata.drop_all(bind=self.engine)
            logger.info("Database tables dropped successfully")
        except Exception as e:
            logger.error("Failed to drop database tables", error=str(e))
            raise

    def health_check(self) -> bool:
        """
        Check database connection health.

        Returns:
            True if database is accessible, False otherwise
        """
        if not self.engine:
            return False

        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.debug("Database health check passed")
            return True
        except Exception as e:
            logger.error("Database health check failed", error=str(e))
            return False

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get database connection information.

        Returns:
            Dictionary with connection details
        """
        if not self.engine:
            return {"status": "not_initialized"}

        return {
            "status": "connected",
            "url": (
                self.database_url.split("@")[1]
                if "@" in self.database_url
                else "configured"
            ),
            "pool_size": self.engine.pool.size(),
            "checked_in": self.engine.pool.checkedin(),
            "checked_out": self.engine.pool.checkedout(),
            "overflow": self.engine.pool.overflow(),
        }

    def close(self) -> None:
        """Close database connections."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")


# Global database client instance (lazy-loaded)
db_client: Optional[DatabaseClient] = None


def get_database_client() -> DatabaseClient:
    """
    Get the global database client instance.

    Returns:
        Database client instance
    """
    global db_client
    if db_client is None:
        db_client = DatabaseClient()
    return db_client


def get_db_session() -> Generator[Session, None, None]:
    """
    Get database session from global client.

    Yields:
        SQLAlchemy session

    Example:
        from core.clients.database import get_db_session

        with get_db_session() as session:
            trip = session.query(CompleteTrip).first()
    """
    with db_client.get_session() as session:
        yield session
