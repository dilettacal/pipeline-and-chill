"""
Infrastructure tests for ChillFlow platform using Testcontainers.

These tests use Testcontainers to spin up real infrastructure services
in isolated Docker containers for testing.
"""

import os
import time
from datetime import datetime

import pytest
from core import CompleteTripSchema
from core.clients.database import DatabaseClient
from core.clients.kafka import KafkaClient
from core.clients.redis import RedisClient
from sqlalchemy import text
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


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
def postgres_container():
    """Provide a PostgreSQL container for infrastructure tests."""
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def redis_container():
    """Provide a Redis container for infrastructure tests."""
    with RedisContainer("redis:7-alpine") as redis:
        yield redis


@pytest.fixture(scope="session")
def kafka_container():
    """Provide a Kafka container for infrastructure tests."""
    image = os.getenv("KAFKA_TESTCONTAINERS_IMAGE", "confluentinc/cp-kafka:7.6.1")
    with KafkaContainer(image=image) as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _wait_for_kafka(bootstrap, timeout=90)
        yield kafka


@pytest.fixture
def db_connection(postgres_container):
    """Database connection for infrastructure tests."""
    # Create database client with container connection
    db_url = postgres_container.get_connection_url()

    # Configure global database client to use Testcontainer BEFORE migrations
    import os

    os.environ["DATABASE_URL"] = db_url

    db_client = DatabaseClient(db_url)

    # Setup database schema
    from core.migrations.setup_database import seed_zones_data, setup_database_schema

    setup_database_schema(db_client.engine)
    seed_zones_data(db_client.engine)

    return db_client


@pytest.fixture
def redis_connection(redis_container):
    """Redis connection for infrastructure tests."""
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    return RedisClient(redis_url)


@pytest.fixture
def kafka_connection(kafka_container):
    """Kafka connection for infrastructure tests."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    return KafkaClient(bootstrap_servers)


@pytest.mark.infrastructure
class TestPlatformInfrastructure:
    """Test platform infrastructure components using Testcontainers."""

    def test_database_connection(self, db_connection):
        """Test PostgreSQL database connection using Testcontainers."""
        # Test health check
        assert db_connection.health_check(), "Database health check failed"

        # Test connection info
        info = db_connection.get_connection_info()
        assert info["status"] == "connected"

        # Test session using the Testcontainer client directly
        with db_connection.get_session() as session:
            result = session.execute(text("SELECT 1 as test")).fetchone()
            assert result and result[0] == 1, "Database query failed"

    def test_redis_connection(self, redis_connection):
        """Test Redis connection using Testcontainers."""
        # Test health check
        assert redis_connection.health_check(), "Redis health check failed"

        # Test basic operations
        redis_connection.set("test:chillflow", "infrastructure_test", ex=60)
        value = redis_connection.get("test:chillflow")

        assert value and value.decode() == "infrastructure_test", "Redis read/write failed"

    def test_database_models(self, db_connection):
        """Test database models and schemas using Testcontainers."""
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

        # Test database session with models using Testcontainer client directly
        with db_connection.get_session() as session:
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

    def test_kafka_connection(self, kafka_connection):
        """Test Kafka connection using Testcontainers."""
        # Test health check
        assert kafka_connection.health_check(), "Kafka health check failed"

        # Test producer
        producer = kafka_connection.get_producer()
        success = producer.send_message(
            "test-topic",
            {
                "message": "ChillFlow infrastructure test",
                "timestamp": datetime.now().isoformat(),
            },
        )

        assert success, "Kafka producer failed"
