"""
Unified Testcontainers configuration for ChillFlow Stream tests.

This implements the expert's approach with a single, clean Testcontainers strategy.
"""

import os
import time
import uuid
from datetime import datetime, timedelta

import pytest
from core import CompleteTrip
from testcontainers.postgres import PostgresContainer


def _wait_for_kafka(bootstrap: str, timeout=60):
    """Wait for Kafka to be ready by checking cluster info."""
    from kafka import KafkaAdminClient

    start = time.time()
    last_err = None
    while time.time() - start < timeout:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="test-admin")
            admin.list_topics()
            admin.close()
            return
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise TimeoutError(f"Kafka not ready at {bootstrap}: {last_err}")


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    """
    Always start a Kafka broker with Testcontainers for this test session.
    Returns a usable bootstrap server string (host:port).
    """
    from testcontainers.kafka import KafkaContainer

    # Pin to a specific, known-fast image to avoid surprises
    image = os.getenv("KAFKA_TESTCONTAINERS_IMAGE", "confluentinc/cp-kafka:7.6.1")

    # NOTE: session-scoped context manager keeps a single broker for all tests
    with KafkaContainer(image=image) as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _wait_for_kafka(bootstrap, timeout=90)
        yield bootstrap
        # container stops automatically


@pytest.fixture(scope="session", autouse=True)
def log_bootstrap(kafka_bootstrap):
    print(f"\n[pytest] Testcontainers Kafka at {kafka_bootstrap}\n", flush=True)


@pytest.fixture(scope="session")
def postgres_container():
    """
    Provide a PostgreSQL container for database integration tests.
    """
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture
def db_connection(postgres_container):
    """Database connection for integration tests."""
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
# Kafka Integration Fixtures
# =============================================================================


@pytest.fixture
def kafka_bootstrap_servers(kafka_bootstrap):
    """Kafka bootstrap servers for integration tests."""
    return kafka_bootstrap


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


@pytest.fixture
def ensure_kafka_topic(kafka_bootstrap_servers, kafka_topic):
    """Ensure Kafka topic exists before tests run."""
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers, client_id="test-admin")
    existing = set(admin.list_topics())

    if kafka_topic not in existing:
        try:
            admin.create_topics(
                [NewTopic(name=kafka_topic, num_partitions=1, replication_factor=1)]
            )
        except Exception:
            # Topic might appear due to race; ignore if already exists
            pass

    admin.close()
    return kafka_topic


# =============================================================================
# Test Markers
# =============================================================================


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests (no external dependencies)")
    config.addinivalue_line("markers", "integration: Integration tests (require testcontainers)")
    config.addinivalue_line("markers", "e2e: End-to-end tests (full pipeline)")
    config.addinivalue_line("markers", "slow: Slow-running tests")
