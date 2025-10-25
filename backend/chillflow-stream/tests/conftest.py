"""
Pytest configuration and shared fixtures for ChillFlow Stream tests.

This file provides testcontainers-based fixtures for proper integration testing
with real Kafka and PostgreSQL instances.
"""

import os
import uuid
from datetime import datetime, timedelta
from typing import Generator

import pytest
from core import CompleteTrip
from testcontainers.compose import DockerCompose
from testcontainers.kafka import RedpandaContainer
from testcontainers.postgres import PostgresContainer

# =============================================================================
# Test Environment Configuration
# =============================================================================


@pytest.fixture(scope="session")
def test_env():
    """Test environment configuration."""
    return {
        "KAFKA_BOOTSTRAP": "localhost:9092",
        "POSTGRES_URL": "postgresql://test:test@localhost:5432/test",
        "REDIS_URL": "redis://localhost:6379/1",
    }


# =============================================================================
# Testcontainers Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def kafka_container():
    """
    Provide a Redpanda container for Kafka integration tests.

    Uses RedpandaContainer which is Kafka-compatible but faster for testing.
    """
    with RedpandaContainer() as redpanda:
        yield redpanda


@pytest.fixture(scope="session")
def postgres_container():
    """
    Provide a PostgreSQL container for database integration tests.
    """
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def docker_compose():
    """
    Provide full infrastructure stack using docker-compose.

    This starts all services (Kafka, PostgreSQL, Redis) together.
    """
    compose_file = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "infra", "docker-compose.yml"
    )

    with DockerCompose(compose_file) as compose:
        # Wait for services to be ready
        compose.wait_for("kafka:9092")
        compose.wait_for("postgres:5432")
        compose.wait_for("redis:6379")
        yield compose


# =============================================================================
# Database Fixtures
# =============================================================================


@pytest.fixture
def db_connection(postgres_container):
    """Database connection for integration tests."""
    import psycopg
    from sqlalchemy import create_engine

    connection_url = postgres_container.get_connection_url()
    engine = create_engine(connection_url)

    yield engine

    engine.dispose()


@pytest.fixture
def db_session(db_connection):
    """Database session with transaction rollback."""
    from sqlalchemy.orm import sessionmaker

    Session = sessionmaker(bind=db_connection)
    session = Session()

    yield session

    session.rollback()
    session.close()


# =============================================================================
# Kafka Fixtures
# =============================================================================


@pytest.fixture
def kafka_bootstrap_servers(kafka_container):
    """Kafka bootstrap servers for integration tests."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture
def kafka_producer(kafka_bootstrap_servers):
    """Kafka producer for integration tests."""
    import json

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )

    yield producer

    producer.close()


@pytest.fixture
def kafka_consumer(kafka_bootstrap_servers, kafka_topic):
    """Kafka consumer for integration tests."""
    import json

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=f"test-group-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=5000,
    )

    yield consumer

    consumer.close()


@pytest.fixture
def kafka_topic():
    """Unique test topic name to avoid message cross-contamination."""
    return f"test-trips-{uuid.uuid4().hex[:8]}"


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def sample_trip_data():
    """Sample trip data for testing."""
    pickup_ts = datetime(2025, 1, 1, 10, 0, 0)
    dropoff_ts = pickup_ts + timedelta(minutes=30)

    return {
        "trip_key": f"test-trip-{uuid.uuid4().hex[:8]}",
        "vendor_id": 1,
        "pickup_ts": pickup_ts,
        "dropoff_ts": dropoff_ts,
        "pu_zone_id": 1,
        "do_zone_id": 2,
        "passenger_count": 1,
        "trip_distance": 5.5,
        "fare_amount": 15.50,
        "tip_amount": 3.00,
        "total_amount": 18.50,
        "payment_type": 1,
        "vehicle_id_h": f"vehicle-{uuid.uuid4().hex[:8]}",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }


@pytest.fixture
def sample_complete_trip(sample_trip_data):
    """Sample CompleteTrip object for testing."""
    return CompleteTrip(**sample_trip_data)


@pytest.fixture
def sample_events():
    """Sample events for testing."""
    trip_key = f"test-trip-{uuid.uuid4().hex[:8]}"
    base_timestamp = datetime(2025, 1, 1, 10, 0, 0)

    return [
        {
            "event_id": f"event-{uuid.uuid4().hex[:8]}",
            "event_type": "trip_started",
            "trip_key": trip_key,
            "vendor_id": 1,
            "timestamp": base_timestamp.isoformat(),
            "pickup_zone_id": 1,
            "passenger_count": 1,
            "vehicle_id_h": f"vehicle-{uuid.uuid4().hex[:8]}",
        },
        {
            "event_id": f"event-{uuid.uuid4().hex[:8]}",
            "event_type": "trip_ended",
            "trip_key": trip_key,
            "vendor_id": 1,
            "timestamp": (base_timestamp + timedelta(minutes=30)).isoformat(),
            "dropoff_zone_id": 2,
            "trip_distance": 5.5,
            "vehicle_id_h": f"vehicle-{uuid.uuid4().hex[:8]}",
        },
        {
            "event_id": f"event-{uuid.uuid4().hex[:8]}",
            "event_type": "payment_processed",
            "trip_key": trip_key,
            "vendor_id": 1,
            "timestamp": (base_timestamp + timedelta(minutes=30)).isoformat(),
            "fare_amount": 15.50,
            "tip_amount": 3.00,
            "total_amount": 18.50,
            "payment_type": 1,
            "vehicle_id_h": f"vehicle-{uuid.uuid4().hex[:8]}",
        },
    ]


# =============================================================================
# Test Markers
# =============================================================================


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests (no external dependencies)")
    config.addinivalue_line("markers", "integration: Integration tests (require testcontainers)")
    config.addinivalue_line("markers", "e2e: End-to-end tests (full pipeline)")
    config.addinivalue_line("markers", "slow: Slow-running tests")


def pytest_collection_modifyitems(config, items):
    """
    Automatically skip integration tests if testcontainers not available.
    """
    skip_integration = pytest.mark.skip(
        reason="testcontainers not available or integration test requires Docker"
    )

    for item in items:
        # Skip integration tests if testcontainers not available
        if "integration" in item.keywords:
            try:
                import testcontainers
            except ImportError:
                item.add_marker(skip_integration)
