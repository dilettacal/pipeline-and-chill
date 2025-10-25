"""
Infrastructure tests for ChillFlow platform.

These tests require Docker infrastructure to be running.
Run with: pytest tests/infrastructure/ -m infrastructure
"""

from datetime import datetime

import pytest
from core import (
    CompleteTrip,
    CompleteTripSchema,
    Zone,
    get_database_client,
    get_kafka_client,
    get_logger,
    get_redis_client,
    setup_development_logging,
)
from core.clients.database import get_db_session
from sqlalchemy import text


@pytest.mark.infrastructure
class TestPlatformInfrastructure:
    """Test platform infrastructure components."""

    def test_database_connection(self):
        """Test PostgreSQL database connection."""
        db_client = get_database_client()

        # Test health check
        assert db_client.health_check(), "Database health check failed"

        # Test connection info
        info = db_client.get_connection_info()
        assert info["status"] == "connected"

        # Test session
        session_gen = get_db_session()
        session = next(session_gen)
        try:
            result = session.execute(text("SELECT 1 as test")).fetchone()
            assert result and result[0] == 1, "Database query failed"
        finally:
            session.close()

    def test_redis_connection(self):
        """Test Redis connection."""
        redis_client = get_redis_client()

        # Test health check
        assert redis_client.health_check(), "Redis health check failed"

        # Test basic operations
        redis_client.set("test:chillflow", "infrastructure_test", ex=60)
        value = redis_client.get("test:chillflow")

        assert value and value.decode() == "infrastructure_test", "Redis read/write failed"

    def test_database_models(self):
        """Test database models and schemas."""
        # Test schema validation
        pickup_time = datetime.now()
        dropoff_time = datetime.now()
        # Ensure dropoff is after pickup
        if dropoff_time <= pickup_time:
            dropoff_time = pickup_time.replace(second=pickup_time.second + 1)

        trip_schema = CompleteTripSchema(
            trip_key="test_trip_123",
            vendor_id=1,
            pickup_ts=pickup_time,
            dropoff_ts=dropoff_time,
            pu_zone_id=229,
            do_zone_id=230,
            vehicle_id_h="test_vehicle",
        )

        assert trip_schema.trip_key == "test_trip_123"

        # Test database session with models
        session_gen = get_db_session()
        session = next(session_gen)
        try:
            # Test that we can query the database
            result = session.execute(
                text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dim'")
            ).fetchone()
            dim_tables = result[0] if result else 0

            result = session.execute(
                text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'stg'")
            ).fetchone()
            stg_tables = result[0] if result else 0

            assert dim_tables >= 1, "dim schema not found"
            assert stg_tables >= 1, "stg schema not found"
        finally:
            session.close()

    @pytest.mark.skip(reason="Kafka client health check needs refinement")
    def test_kafka_connection(self):
        """Test Kafka connection."""
        kafka_client = get_kafka_client()

        # Test health check
        assert kafka_client.health_check(), "Kafka health check failed"

        # Test producer
        producer = kafka_client.get_producer()
        success = producer.send_message(
            "test-topic",
            {
                "message": "ChillFlow infrastructure test",
                "timestamp": datetime.now().isoformat(),
            },
        )

        assert success, "Kafka producer failed"
