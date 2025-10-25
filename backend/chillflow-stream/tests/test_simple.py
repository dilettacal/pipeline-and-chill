"""Simple tests for stream components."""

from datetime import datetime
from unittest.mock import patch

from stream.events import EventType, TripStartedEvent
from stream.trip_assembler import TripAssembler
from stream.trip_event_producer import TripEventProducer


class TestSimpleStreamComponents:
    """Simple tests for stream components."""

    def test_event_creation(self):
        """Test creating events with required fields."""
        event = TripStartedEvent(
            event_id="test-event-1",
            trip_key="test-trip",
            vendor_id=1,
            timestamp=datetime.now(),
            pickup_zone_id=1,
            passenger_count=1,
            vehicle_id_h="vehicle-123",
        )

        assert event.event_type == EventType.TRIP_STARTED
        assert event.trip_key == "test-trip"
        assert event.vendor_id == 1

    def test_trip_event_producer_initialization(self):
        """Test trip event producer initialization."""
        with patch("stream.trip_event_producer.KafkaProducer"):
            producer = TripEventProducer()
            assert hasattr(producer, "kafka_producer")

    def test_trip_assembler_initialization(self):
        """Test trip assembler initialization."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()
            assert hasattr(assembler, "kafka_consumer")
            assert len(assembler.partial_trips) == 0
            assert len(assembler.completed_trips) == 0

    def test_event_serialization(self):
        """Test event serialization to dict."""
        event = TripStartedEvent(
            event_id="test-event-1",
            trip_key="test-trip",
            vendor_id=1,
            timestamp=datetime.now(),
            pickup_zone_id=1,
            passenger_count=1,
            vehicle_id_h="vehicle-123",
        )

        # Use Pydantic's model_dump method
        event_dict = event.model_dump()
        assert isinstance(event_dict, dict)
        assert event_dict["event_type"] == "trip_started"
        assert event_dict["trip_key"] == "test-trip"

    def test_event_deserialization(self):
        """Test event deserialization from dict."""
        event_data = {
            "event_id": "test-event-1",
            "event_type": "trip_started",
            "trip_key": "test-trip",
            "vendor_id": 1,
            "timestamp": "2025-01-01T10:00:00",
            "pickup_zone_id": 1,
            "passenger_count": 1,
            "vehicle_id_h": "vehicle-123",
        }

        event = TripStartedEvent(**event_data)
        assert event.event_type == EventType.TRIP_STARTED
        assert event.trip_key == "test-trip"

    def test_assembler_partial_trip_management(self):
        """Test assembler partial trip management."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            # Test initial state
            assert assembler.get_partial_trip_count() == 0
            assert assembler.get_completed_trip_count() == 0

            # Test adding partial trip
            assembler.partial_trips["test-trip"] = {}
            assert assembler.get_partial_trip_count() == 1

            # Test adding completed trip
            from core import CompleteTrip

            assembler.completed_trips.append(
                CompleteTrip(
                    trip_key="test-trip",
                    vendor_id=1,
                    pickup_ts=datetime.now(),
                    dropoff_ts=datetime.now(),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            assert assembler.get_completed_trip_count() == 1
