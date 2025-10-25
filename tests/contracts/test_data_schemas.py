"""
Contract tests for data schemas and transformations.

These tests verify that data schemas are consistent across services
and that transformations work correctly.
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest

# Add backend paths for imports
backend_path = Path(__file__).parent.parent.parent / "backend"
sys.path.insert(0, str(backend_path / "chillflow-core"))
sys.path.insert(0, str(backend_path / "chillflow-batch"))
sys.path.insert(0, str(backend_path / "chillflow-stream"))

from core import CompleteTrip
from stream.events import TripStartedEvent


@pytest.mark.contract
class TestDataSchemas:
    """Test data schema contracts between services."""

    def test_complete_trip_schema_consistency(self):
        """Test that CompleteTrip schema is consistent across services."""
        # Test data
        trip_data = {
            "trip_key": "test-trip-123",
            "vendor_id": 1,
            "pickup_ts": datetime(2025, 1, 1, 10, 0, 0),
            "dropoff_ts": datetime(2025, 1, 1, 10, 30, 0),
            "pu_zone_id": 1,
            "do_zone_id": 2,
            "passenger_count": 1,
            "trip_distance": 5.0,
            "fare_amount": 10.0,
            "total_amount": 12.0,
            "payment_type": 1,
            "tip_amount": 2.0,
            "vehicle_id_h": "vehicle-123",
        }

        # Test Pydantic model creation
        trip = CompleteTrip(**trip_data)
        assert trip.trip_key == "test-trip-123"
        assert trip.vendor_id == 1
        assert trip.fare_amount == 10.0

        # Test that the model was created successfully
        assert trip.trip_key == "test-trip-123"
        assert trip.vendor_id == 1
        assert trip.fare_amount == 10.0

    def test_event_schema_consistency(self):
        """Test that event schemas are consistent."""
        # Test TripStartedEvent
        started_event = TripStartedEvent(
            event_id="event-123",
            event_type="trip_started",
            trip_key="trip-123",
            vendor_id=1,
            timestamp=datetime.now().isoformat(),
            pickup_zone_id=1,
            passenger_count=1,
            vehicle_id_h="vehicle-123",
        )

        assert started_event.event_type == "trip_started"
        assert started_event.trip_key == "trip-123"

        # Test serialization
        event_dict = started_event.model_dump()
        assert isinstance(event_dict, dict)
        assert event_dict["event_type"] == "trip_started"

    def test_optional_fields_handling(self):
        """Test that optional fields are handled correctly."""
        # Test with minimal required fields
        minimal_event = TripStartedEvent(
            event_id="event-123",
            event_type="trip_started",
            trip_key="trip-123",
            vendor_id=1,
            timestamp=datetime.now().isoformat(),
            pickup_zone_id=1,
            passenger_count=1,
            vehicle_id_h="vehicle-123",
        )

        assert minimal_event.pickup_zone_id == 1

        # Test with None values for optional fields
        event_with_none = TripStartedEvent(
            event_id="event-123",
            event_type="trip_started",
            trip_key="trip-123",
            vendor_id=1,
            timestamp=datetime.now().isoformat(),
            pickup_zone_id=None,  # This should be allowed
            passenger_count=1,
            vehicle_id_h="vehicle-123",
        )

        assert event_with_none.pickup_zone_id is None

    def test_data_transformation_contracts(self):
        """Test data transformation between different formats."""
        # Test trip to events transformation
        trip_data = {
            "trip_key": "test-trip-123",
            "vendor_id": 1,
            "pickup_ts": datetime(2025, 1, 1, 10, 0, 0),
            "dropoff_ts": datetime(2025, 1, 1, 10, 30, 0),
            "pu_zone_id": 1,
            "do_zone_id": 2,
            "passenger_count": 1,
            "trip_distance": 5.0,
            "fare_amount": 10.0,
            "total_amount": 12.0,
            "payment_type": 1,
            "tip_amount": 2.0,
            "vehicle_id_h": "vehicle-123",
        }

        trip = CompleteTrip(**trip_data)

        # Test that we can create events from trip data
        started_event = TripStartedEvent(
            event_id="event-123",
            event_type="trip_started",
            trip_key=trip.trip_key,
            vendor_id=trip.vendor_id,
            timestamp=trip.pickup_ts.isoformat(),
            pickup_zone_id=trip.pu_zone_id,
            passenger_count=trip.passenger_count,
            vehicle_id_h="vehicle-123",
        )

        assert started_event.trip_key == trip.trip_key
        assert started_event.vendor_id == trip.vendor_id

    def test_schema_validation(self):
        """Test that schema validation works correctly."""
        # Test valid data
        valid_trip = CompleteTrip(
            trip_key="valid-trip",
            vendor_id=1,
            pickup_ts=datetime.now(),
            dropoff_ts=datetime.now(),
            pu_zone_id=1,
            do_zone_id=2,
            passenger_count=1,
            trip_distance=5.0,
            fare_amount=10.0,
            total_amount=12.0,
            payment_type=1,
            tip_amount=2.0,
            vehicle_id_h="vehicle-123",
        )
        assert valid_trip.trip_key == "valid-trip"

        # Test that we can create another valid trip
        another_trip = CompleteTrip(
            trip_key="another-trip",
            vendor_id=2,
            pickup_ts=datetime.now(),
            dropoff_ts=datetime.now(),
            pu_zone_id=3,
            do_zone_id=4,
            passenger_count=2,
            trip_distance=7.0,
            fare_amount=15.0,
            total_amount=18.0,
            payment_type=2,
            tip_amount=3.0,
            vehicle_id_h="vehicle-456",
        )
        assert another_trip.trip_key == "another-trip"
