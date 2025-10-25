"""Test trip assembler functionality."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from stream.events import EventType, PaymentProcessedEvent, TripEndedEvent, TripStartedEvent
from stream.trip_assembler import TripAssembler


class TestTripAssembler:
    """Test cases for TripAssembler."""

    def test_initialization(self):
        """Test assembler initialization."""
        assembler = TripAssembler()
        assert assembler.topic == "trip-events"
        assert assembler.group_id == "trip-assembler"
        assert len(assembler.partial_trips) == 0
        assert len(assembler.completed_trips) == 0

    @patch("stream.trip_assembler.KafkaConsumer")
    def test_initialization_with_custom_params(self, mock_kafka_consumer):
        """Test assembler initialization with custom parameters."""
        assembler = TripAssembler(
            kafka_bootstrap_servers="kafka:9092", topic="custom-topic", group_id="custom-group"
        )
        assert assembler.topic == "custom-topic"
        assert assembler.group_id == "custom-group"

    def test_parse_event_trip_started(self):
        """Test parsing trip started event."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            event_data = {
                "event_id": "event-123",
                "event_type": "trip_started",
                "trip_key": "test-trip",
                "vendor_id": 1,
                "timestamp": "2025-01-01T10:00:00",
                "pickup_zone_id": 1,
                "passenger_count": 1,
                "vehicle_id_h": "vehicle-123",
            }

            event = assembler._parse_event(event_data)
            assert isinstance(event, TripStartedEvent)
            assert event.trip_key == "test-trip"
            assert event.vendor_id == 1

    def test_parse_event_trip_ended(self):
        """Test parsing trip ended event."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            event_data = {
                "event_id": "event-124",
                "event_type": "trip_ended",
                "trip_key": "test-trip",
                "vendor_id": 1,
                "timestamp": "2025-01-01T10:30:00",
                "dropoff_zone_id": 2,
                "trip_distance": 5.5,
                "vehicle_id_h": "vehicle-123",
            }

            event = assembler._parse_event(event_data)
            assert isinstance(event, TripEndedEvent)
            assert event.trip_key == "test-trip"
            assert event.dropoff_zone_id == 2
            assert event.trip_distance == 5.5

    def test_parse_event_payment_processed(self):
        """Test parsing payment processed event."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            event_data = {
                "event_id": "event-125",
                "event_type": "payment_processed",
                "trip_key": "test-trip",
                "vendor_id": 1,
                "timestamp": "2025-01-01T10:30:00",
                "fare_amount": 15.50,
                "tip_amount": 3.00,
                "total_amount": 18.50,
                "payment_type": 1,
                "vehicle_id_h": "vehicle-123",
            }

            event = assembler._parse_event(event_data)
            assert isinstance(event, PaymentProcessedEvent)
            assert event.trip_key == "test-trip"
            assert event.fare_amount == 15.50
            assert event.total_amount == 18.50

    def test_parse_event_unknown_type(self):
        """Test parsing unknown event type."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            event_data = {"event_type": "unknown_event", "trip_key": "test-trip"}

            event = assembler._parse_event(event_data)
            assert event is None

    def test_process_event_incomplete_trip(self):
        """Test processing event for incomplete trip."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            event_data = {
                "event_id": "event-126",
                "event_type": "trip_started",
                "trip_key": "test-trip",
                "vendor_id": 1,
                "timestamp": "2025-01-01T10:00:00",
                "pickup_zone_id": 1,
                "passenger_count": 1,
                "vehicle_id_h": "vehicle-123",
            }

            complete_trip = assembler.process_event(event_data)
            assert complete_trip is None  # Trip not complete yet
            assert "test-trip" in assembler.partial_trips
            assert "trip_started" in assembler.partial_trips["test-trip"]

    def test_process_event_complete_trip(self):
        """Test processing event for complete trip."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            # Add required events to partial trip
            assembler.partial_trips["test-trip"] = {
                "trip_started": TripStartedEvent(
                    event_id="event-127",
                    trip_key="test-trip",
                    vendor_id=1,
                    timestamp=datetime(2025, 1, 1, 10, 0, 0),
                    pickup_zone_id=1,
                    passenger_count=1,
                    vehicle_id_h="vehicle-123",
                ),
                "trip_ended": TripEndedEvent(
                    event_id="event-128",
                    trip_key="test-trip",
                    vendor_id=1,
                    timestamp=datetime(2025, 1, 1, 10, 30, 0),
                    dropoff_zone_id=2,
                    trip_distance=5.5,
                    vehicle_id_h="vehicle-123",
                ),
                "payment_processed": PaymentProcessedEvent(
                    event_id="event-129",
                    trip_key="test-trip",
                    vendor_id=1,
                    timestamp=datetime(2025, 1, 1, 10, 30, 0),
                    fare_amount=15.50,
                    tip_amount=3.00,
                    total_amount=18.50,
                    payment_type=1,
                    vehicle_id_h="vehicle-123",
                ),
            }

            # Process a zone event to trigger completion
            event_data = {
                "event_id": "event-130",
                "event_type": "zone_entered",
                "trip_key": "test-trip",
                "vendor_id": 1,
                "timestamp": "2025-01-01T10:00:00",
                "zone_id": 1,
                "vehicle_id_h": "vehicle-123",
            }

            complete_trip = assembler.process_event(event_data)
            assert complete_trip is not None
            assert complete_trip.trip_key == "test-trip"
            assert complete_trip.vendor_id == 1
            assert complete_trip.pu_zone_id == 1
            assert complete_trip.do_zone_id == 2
            assert complete_trip.fare_amount == 15.50
            assert complete_trip.total_amount == 18.50

    def test_check_trip_completion_missing_events(self):
        """Test trip completion check with missing events."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            # Add only some required events
            assembler.partial_trips["test-trip"] = {
                "trip_started": TripStartedEvent(
                    event_id="event-131",
                    trip_key="test-trip",
                    vendor_id=1,
                    timestamp=datetime(2025, 1, 1, 10, 0, 0),
                    pickup_zone_id=1,
                    passenger_count=1,
                    vehicle_id_h="vehicle-123",
                )
            }

            complete_trip = assembler._check_trip_completion("test-trip")
            assert complete_trip is None

    def test_check_trip_completion_complete_trip(self):
        """Test trip completion check with all required events."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            # Add all required events
            assembler.partial_trips["test-trip"] = {
                "trip_started": TripStartedEvent(
                    event_id="event-132",
                    trip_key="test-trip",
                    vendor_id=1,
                    timestamp=datetime(2025, 1, 1, 10, 0, 0),
                    pickup_zone_id=1,
                    passenger_count=1,
                    vehicle_id_h="vehicle-123",
                ),
                "trip_ended": TripEndedEvent(
                    event_id="event-133",
                    trip_key="test-trip",
                    vendor_id=1,
                    timestamp=datetime(2025, 1, 1, 10, 30, 0),
                    dropoff_zone_id=2,
                    trip_distance=5.5,
                    vehicle_id_h="vehicle-123",
                ),
                "payment_processed": PaymentProcessedEvent(
                    event_id="event-134",
                    trip_key="test-trip",
                    vendor_id=1,
                    timestamp=datetime(2025, 1, 1, 10, 30, 0),
                    fare_amount=15.50,
                    tip_amount=3.00,
                    total_amount=18.50,
                    payment_type=1,
                    vehicle_id_h="vehicle-123",
                ),
            }

            complete_trip = assembler._check_trip_completion("test-trip")
            assert complete_trip is not None
            assert complete_trip.trip_key == "test-trip"
            assert complete_trip.vendor_id == 1

    @patch("stream.trip_assembler.get_db_session")
    def test_save_trips_to_database(self, mock_get_db_session):
        """Test saving trips to database."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()

            # Mock database session
            mock_session = Mock()
            mock_get_db_session.return_value.__next__.return_value = mock_session

            # Create test trips
            from core import CompleteTrip

            trips = [
                CompleteTrip(
                    trip_key="trip-1",
                    vendor_id=1,
                    pickup_ts=datetime(2025, 1, 1, 10, 0, 0),
                    dropoff_ts=datetime(2025, 1, 1, 10, 30, 0),
                    pu_zone_id=1,
                    do_zone_id=2,
                    passenger_count=1,
                    trip_distance=5.5,
                    fare_amount=15.50,
                    tip_amount=3.00,
                    total_amount=18.50,
                    payment_type=1,
                    vehicle_id_h="vehicle-1",
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            ]

            stats = assembler.save_trips_to_database(trips)

            assert stats["saved"] == 1
            assert stats["failed"] == 0
            mock_session.merge.assert_called_once()
            mock_session.commit.assert_called_once()

    def test_get_partial_trip_count(self):
        """Test getting partial trip count."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()
            assert assembler.get_partial_trip_count() == 0

            assembler.partial_trips["trip-1"] = {}
            assembler.partial_trips["trip-2"] = {}
            assert assembler.get_partial_trip_count() == 2

    def test_get_completed_trip_count(self):
        """Test getting completed trip count."""
        with patch("stream.trip_assembler.KafkaConsumer"):
            assembler = TripAssembler()
            assert assembler.get_completed_trip_count() == 0

            from core import CompleteTrip

            assembler.completed_trips.append(
                CompleteTrip(
                    trip_key="trip-1",
                    vendor_id=1,
                    pickup_ts=datetime.now(),
                    dropoff_ts=datetime.now(),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            assert assembler.get_completed_trip_count() == 1

    @patch("stream.trip_assembler.KafkaConsumer")
    def test_close(self, mock_kafka_consumer):
        """Test closing the assembler."""
        # Create mock consumer instance
        mock_consumer_instance = Mock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        assembler = TripAssembler()
        assembler.close()

        # Verify close was called on Kafka consumer
        mock_consumer_instance.close.assert_called_once()
