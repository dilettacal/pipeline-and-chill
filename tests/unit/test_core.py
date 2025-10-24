"""
Unit tests for ChillFlow core components.

These tests don't require external dependencies like databases or Docker.
"""

from datetime import datetime

import pytest
from core import (
    CompleteTripSchema,
    EventType,
    ZoneSchema,
    generate_trip_key,
    generate_vehicle_id_h,
    settings,
    sha256_hex,
)


@pytest.mark.unit
class TestChillFlowCore:
    """Test ChillFlow core functionality."""

    def test_settings_loading(self):
        """Test that settings are loaded correctly."""
        assert settings.DATABASE_URL is not None
        assert settings.REDIS_URL is not None
        assert settings.KAFKA_BOOTSTRAP_SERVERS is not None
        assert settings.HASH_SALT is not None

    def test_hashing_functions(self):
        """Test hashing utility functions."""
        # Test trip key generation
        trip_key = generate_trip_key(
            salt="test-salt",
            vendor_id=1,
            pickup_ts=datetime(2025, 1, 1, 0, 18, 38),
            pu_zone_id=229,
            row_offset=0,
        )
        assert trip_key is not None
        assert len(trip_key) > 0

        # Test vehicle ID generation
        vehicle_id = generate_vehicle_id_h(
            salt="test-salt", vendor_id=1, row_offset=0, fleet_size=5000
        )
        assert vehicle_id is not None
        assert len(vehicle_id) > 0

        # Test SHA256 function
        hash_result = sha256_hex("test-data")
        assert hash_result is not None
        assert len(hash_result) == 64  # SHA256 hex length

    def test_trip_schema_validation(self):
        """Test Pydantic schema validation."""
        # Valid trip data
        trip_data = {
            "trip_key": "test_trip_123",
            "vendor_id": 1,
            "pickup_ts": datetime.now(),
            "dropoff_ts": datetime.now(),
            "pu_zone_id": 229,
            "do_zone_id": 230,
            "vehicle_id_h": "test_vehicle",
        }

        trip = CompleteTripSchema(**trip_data)
        assert trip.trip_key == "test_trip_123"
        assert trip.vendor_id == 1
        assert trip.pu_zone_id == 229
        assert trip.do_zone_id == 230

    def test_zone_schema_validation(self):
        """Test zone schema validation."""
        zone_data = {
            "zone_id": 1,
            "borough": "Manhattan",
            "zone_name": "Newark Airport",
            "service_zone": "EWR",
        }

        zone = ZoneSchema(**zone_data)
        assert zone.zone_id == 1
        assert zone.borough == "Manhattan"
        assert zone.zone_name == "Newark Airport"

    def test_event_type_enum(self):
        """Test event type enumeration."""
        assert EventType.IDENTITY == "IDENTITY"
        assert EventType.PASSENGERS == "PASSENGERS"
        assert EventType.FARE == "FARE"
        assert EventType.DROP == "DROP"
        assert EventType.TIP == "TIP"
