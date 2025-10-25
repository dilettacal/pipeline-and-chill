"""
Unit tests for TripBuilder.

Tests the data transformation logic without external dependencies.
"""


class TestTripBuilder:
    """Unit tests for TripBuilder."""

    def test_creates_complete_trip(self):
        """Test TripBuilder creates CompleteTrip from snapshot."""
        from stream.trip_builder import TripBuilder

        builder = TripBuilder()

        # Test snapshot data
        snapshot = {
            "trip_key": "test-trip-123",
            "vendor_id": "1",
            "fare_amount": "15.50",
            "passenger_count": "2",
            "tip_amount": "3.00",
            "total_amount": "18.50",
            "trip_started_timestamp": "2024-01-01T12:00:00",
            "trip_ended_timestamp": "2024-01-01T12:30:00",
            "payment_processed_timestamp": "2024-01-01T12:31:00",
        }

        # Should build CompleteTrip
        complete_trip = builder.build("test-trip-123", snapshot)

        assert complete_trip is not None
        assert complete_trip.trip_key == "test-trip-123"
        assert complete_trip.vendor_id == 1
        assert complete_trip.fare_amount == 15.50
        assert complete_trip.passenger_count == 2
        assert complete_trip.tip_amount == 3.00
        assert complete_trip.total_amount == 18.50

    def test_handles_missing_fields(self):
        """Test TripBuilder handles missing fields gracefully."""
        from stream.trip_builder import TripBuilder

        builder = TripBuilder()

        # Test snapshot with missing fields
        snapshot = {
            "trip_key": "test-trip-456",
            "vendor_id": "1",
            "fare_amount": "15.50",
            # Missing: passenger_count, tip_amount, total_amount
        }

        # Should build CompleteTrip with defaults
        complete_trip = builder.build("test-trip-456", snapshot)

        assert complete_trip is not None
        assert complete_trip.trip_key == "test-trip-456"
        assert complete_trip.vendor_id == 1
        assert complete_trip.fare_amount == 15.50
        assert complete_trip.passenger_count == 0  # Default
        assert complete_trip.tip_amount == 0.0  # Default
        assert complete_trip.total_amount == 0.0  # Default

    def test_handles_invalid_data(self):
        """Test TripBuilder handles invalid data gracefully."""
        from stream.trip_builder import TripBuilder

        builder = TripBuilder()

        # Test snapshot with invalid data
        snapshot = {
            "trip_key": "test-trip-invalid",
            "vendor_id": "invalid",
            "fare_amount": "not-a-number",
            "passenger_count": "also-invalid",
        }

        # Should build CompleteTrip with defaults for invalid fields
        complete_trip = builder.build("test-trip-invalid", snapshot)

        assert complete_trip is not None
        assert complete_trip.trip_key == "test-trip-invalid"
        assert complete_trip.vendor_id == 0  # Default for invalid
        assert complete_trip.fare_amount == 0.0  # Default for invalid
        assert complete_trip.passenger_count == 0  # Default for invalid

    def test_empty_snapshot(self):
        """Test TripBuilder handles empty snapshot."""
        from stream.trip_builder import TripBuilder

        builder = TripBuilder()

        # Test with empty snapshot
        snapshot = {}

        # Should build CompleteTrip with all defaults
        complete_trip = builder.build("test-trip-empty", snapshot)

        assert complete_trip is not None
        assert complete_trip.trip_key == "test-trip-empty"
        assert complete_trip.vendor_id == 0
        assert complete_trip.fare_amount == 0.0
        assert complete_trip.passenger_count == 0
        assert complete_trip.tip_amount == 0.0
        assert complete_trip.total_amount == 0.0
