"""
Unit tests for TripAssemblerProcessor.

Tests the core business logic for trip processing without external dependencies.
"""

from unittest.mock import Mock

from stream.events import EventType


class TestTripAssemblerProcessor:
    """Unit tests for TripAssemblerProcessor."""

    def test_initialization(self):
        """Test TripAssemblerProcessor initializes correctly."""
        from stream.completion_policy import CompletionPolicy
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Mock dependencies
        mock_state_manager = Mock()
        builder = TripBuilder()
        policy = CompletionPolicy()

        # Should initialize without errors
        processor = TripAssemblerProcessor(mock_state_manager, builder, policy)

        assert processor.state_manager == mock_state_manager
        assert processor.builder == builder
        assert processor.policy == policy

    def test_parse_valid_events(self):
        """Test TripAssemblerProcessor parses valid events correctly."""
        from stream.completion_policy import CompletionPolicy
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Mock dependencies
        mock_state_manager = Mock()
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(mock_state_manager, builder, policy)

        # Test trip_started event
        event_data = {
            "event_id": "test-1",
            "trip_key": "test-trip-123",
            "event_type": "trip_started",
            "timestamp": "2024-01-01T12:00:00",
            "vendor_id": 1,
            "vehicle_id_h": "vehicle-123",
            "pickup_zone_id": 100,
        }

        parsed_event = processor._parse_event(event_data)
        assert parsed_event is not None
        assert parsed_event.event_type == EventType.TRIP_STARTED
        assert parsed_event.trip_key == "test-trip-123"

    def test_parse_invalid_events(self):
        """Test TripAssemblerProcessor handles invalid events."""
        from stream.completion_policy import CompletionPolicy
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Mock dependencies
        mock_state_manager = Mock()
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(mock_state_manager, builder, policy)

        # Test unknown event type
        invalid_event = {
            "trip_key": "invalid-test",
            "event_type": "unknown_event_type",
            "timestamp": "2024-01-01T12:00:00",
        }

        parsed_event = processor._parse_event(invalid_event)
        assert parsed_event is None

        # Test malformed event
        malformed_event = {
            "trip_key": "malformed-test",
            # Missing required fields
        }

        parsed_event = processor._parse_event(malformed_event)
        assert parsed_event is None

    def test_process_incomplete_trip(self):
        """Test TripAssemblerProcessor handles incomplete trips."""
        from stream.completion_policy import CompletionPolicy
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Mock dependencies
        mock_state_manager = Mock()
        mock_state_manager.upsert_event_and_check_complete.return_value = (False, {})

        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(mock_state_manager, builder, policy)

        # Test event data
        event_data = {
            "event_id": "test-1",
            "trip_key": "test-trip-123",
            "event_type": "trip_started",
            "timestamp": "2024-01-01T12:00:00",
            "vendor_id": 1,
            "vehicle_id_h": "vehicle-123",
            "pickup_zone_id": 100,
        }

        # Should return None for incomplete trip
        result = processor.process(event_data)
        assert result is None

        # Should call state manager
        mock_state_manager.upsert_event_and_check_complete.assert_called_once()

    def test_process_complete_trip(self):
        """Test TripAssemblerProcessor handles complete trips."""
        from core import CompleteTrip

        from stream.completion_policy import CompletionPolicy
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Mock dependencies
        mock_state_manager = Mock()
        mock_state_manager.upsert_event_and_check_complete.return_value = (
            True,
            {"vendor_id": "1", "fare_amount": "15.50"},
        )

        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(mock_state_manager, builder, policy)

        # Test event data
        event_data = {
            "event_id": "test-1",
            "trip_key": "test-trip-123",
            "event_type": "trip_started",
            "timestamp": "2024-01-01T12:00:00",
            "vendor_id": 1,
            "vehicle_id_h": "vehicle-123",
            "pickup_zone_id": 100,
        }

        # Should return CompleteTrip for complete trip
        result = processor.process(event_data)
        assert result is not None
        assert isinstance(result, CompleteTrip)
        assert result.trip_key == "test-trip-123"

    def test_process_invalid_event(self):
        """Test TripAssemblerProcessor handles invalid events gracefully."""
        from stream.completion_policy import CompletionPolicy
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Mock dependencies
        mock_state_manager = Mock()
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(mock_state_manager, builder, policy)

        # Test invalid event
        invalid_event = {
            "trip_key": "invalid-test",
            "event_type": "unknown_event_type",
            "timestamp": "2024-01-01T12:00:00",
        }

        # Should return None for invalid event
        result = processor.process(invalid_event)
        assert result is None

        # Should not call state manager for invalid events
        mock_state_manager.upsert_event_and_check_complete.assert_not_called()
