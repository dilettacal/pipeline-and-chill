"""
Simple integration tests for ChillFlow Batch using testcontainers.

These tests demonstrate that testcontainers work with the batch service
without complex database operations.
"""

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.mark.integration
class TestBatchSimpleIntegration:
    """Simple integration tests for batch processing."""

    def test_postgres_container_starts(self):
        """Test that PostgreSQL container can be started."""
        with PostgresContainer("postgres:15") as postgres:
            # Test that we can connect to the container
            connection_url = postgres.get_connection_url()
            assert connection_url is not None
            assert "postgresql" in connection_url

    def test_postgres_container_with_custom_database(self):
        """Test PostgreSQL container with custom database."""
        with PostgresContainer("postgres:15") as postgres:
            # Test basic connection
            connection_url = postgres.get_connection_url()
            assert connection_url is not None

            # Test that we can get container host IP
            host_ip = postgres.get_container_host_ip()
            assert host_ip is not None

    def test_postgres_container_environment_variables(self):
        """Test that PostgreSQL container sets environment variables correctly."""
        with PostgresContainer("postgres:15") as postgres:
            # Test that we can get the connection URL (which includes env vars)
            connection_url = postgres.get_connection_url()
            assert connection_url is not None
            # The URL should contain the default test credentials
            assert "test:test" in connection_url

    def test_batch_service_dependencies_available(self):
        """Test that batch service dependencies are available."""
        # Test that we can import batch modules
        from batch.aggregator import BatchAggregator
        from batch.loader import MonthlyLoader
        from batch.producer import BatchTripProducer

        # Test that we can create instances (with mocked dependencies)
        with pytest.MonkeyPatch().context() as m:
            m.setattr("batch.producer.get_database_client", lambda: None)
            m.setattr("batch.producer.settings", type("Settings", (), {"HASH_SALT": "test"})())

            producer = BatchTripProducer()
            assert producer is not None

            aggregator = BatchAggregator()
            assert aggregator is not None

            loader = MonthlyLoader()
            assert loader is not None

    def test_testcontainers_imports(self):
        """Test that testcontainers modules are available."""
        from testcontainers.postgres import PostgresContainer

        # Test that we can create container instances
        postgres = PostgresContainer("postgres:15")
        assert postgres is not None

        # Test that we can get container host IP
        host_ip = postgres.get_container_host_ip()
        assert host_ip is not None
