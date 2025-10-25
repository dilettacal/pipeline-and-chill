"""
Pytest configuration and shared fixtures for ChillFlow Batch tests.

This file provides testcontainers-based fixtures for proper integration testing
with real PostgreSQL instances.
"""

import os
import uuid
from datetime import datetime, timedelta
from typing import Generator

import pytest
from core import CompleteTrip
from testcontainers.postgres import PostgresContainer

# =============================================================================
# Test Environment Configuration
# =============================================================================


@pytest.fixture(scope="session")
def test_env():
    """Test environment configuration."""
    return {
        "POSTGRES_URL": "postgresql://test:test@localhost:5432/test",
        "HASH_SALT": "test-salt-do-not-use-in-production",
    }


# =============================================================================
# Testcontainers Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def postgres_container():
    """
    Provide a PostgreSQL container for database integration tests.
    """
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


# =============================================================================
# Database Fixtures
# =============================================================================


@pytest.fixture
def db_connection(postgres_container):
    """Database connection for integration tests."""
    import psycopg
    from sqlalchemy import create_engine

    # Get the connection URL and ensure it uses the psycopg driver
    connection_url = postgres_container.get_connection_url()
    if connection_url.startswith("postgresql://"):
        connection_url = connection_url.replace("postgresql://", "postgresql+psycopg://")

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
        "pu_zone_id": 229,
        "do_zone_id": 230,
        "passenger_count": 1,
        "rate_code": 1,
        "store_and_fwd_flag": "N",
        "fare_amount": 10.0,
        "total_amount": 12.0,
        "payment_type": 1,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "distance_km": 5.0,
        "duration_min": 30.0,
        "avg_speed_kmh": 10.0,
        "vehicle_id_h": f"vehicle-{uuid.uuid4().hex[:8]}",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }


@pytest.fixture
def sample_complete_trip(sample_trip_data):
    """Sample CompleteTrip object for testing."""
    return CompleteTrip(**sample_trip_data)


@pytest.fixture
def sample_trips_dataframe():
    """Sample DataFrame with multiple trips for testing."""
    import pandas as pd

    trips_data = [
        {
            "vendor_id": 1,
            "pickup_ts": datetime(2025, 1, 1, 10, 0, 0),
            "dropoff_ts": datetime(2025, 1, 1, 10, 30, 0),
            "pu_zone_id": 229,
            "do_zone_id": 230,
            "passenger_count": 1,
            "rate_code": 1,
            "store_and_fwd_flag": "N",
            "fare_amount": 10.0,
            "total_amount": 12.0,
            "payment_type": 1,
            "tip_amount": 2.0,
            "tolls_amount": 0.0,
            "congestion_surcharge": 0.0,
            "airport_fee": 0.0,
            "distance_km": 5.0,
            "duration_min": 30.0,
            "avg_speed_kmh": 10.0,
        },
        {
            "vendor_id": 2,
            "pickup_ts": datetime(2025, 1, 1, 11, 0, 0),
            "dropoff_ts": datetime(2025, 1, 1, 11, 30, 0),
            "pu_zone_id": 230,
            "do_zone_id": 231,
            "passenger_count": 2,
            "rate_code": 1,
            "store_and_fwd_flag": "N",
            "fare_amount": 15.0,
            "total_amount": 18.0,
            "payment_type": 1,
            "tip_amount": 3.0,
            "tolls_amount": 0.0,
            "congestion_surcharge": 0.0,
            "airport_fee": 0.0,
            "distance_km": 8.0,
            "duration_min": 30.0,
            "avg_speed_kmh": 16.0,
        },
    ]

    return pd.DataFrame(trips_data)


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
