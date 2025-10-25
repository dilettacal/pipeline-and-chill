"""Test stream processing integration."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from stream.trip_assembler import TripAssembler
from stream.trip_event_producer import TripEventProducer


class TestStreamIntegration:
    """Test end-to-end stream processing integration."""

    @patch("stream.trip_event_producer.KafkaProducer")
    @patch("stream.trip_assembler.KafkaConsumer")
    def test_producer_to_assembler_flow(self, mock_kafka_consumer, mock_kafka_producer):
        """Test complete flow from producer to assembler."""
        # Create producer
        producer = TripEventProducer()

        # Create test trip data as CompleteTrip object
        from core import CompleteTrip

        trip_data = CompleteTrip(
            trip_key="integration-test-trip",
            vendor_id=2,
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
            vehicle_id_h="vehicle-integration",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Generate events
        events = producer.process_trip(trip_data)
        assert len(events) == 5

        # Create assembler
        assembler = TripAssembler()

        # Process events one by one
        complete_trip = None
        for event in events:
            complete_trip = assembler.process_event(event.model_dump())
            if complete_trip:
                break

        # Should have assembled the trip
        assert complete_trip is not None
        assert complete_trip.trip_key == "integration-test-trip"
        assert complete_trip.vendor_id == 2
        assert complete_trip.pu_zone_id == 1
        assert complete_trip.do_zone_id == 2
        assert complete_trip.fare_amount == 15.50
        assert complete_trip.total_amount == 18.50

    @patch("stream.trip_event_producer.KafkaProducer")
    @patch("stream.trip_assembler.KafkaConsumer")
    def test_multiple_trips_processing(self, mock_kafka_consumer, mock_kafka_producer):
        """Test processing multiple trips."""
        producer = TripEventProducer()
        assembler = TripAssembler()

        # Create multiple trips as CompleteTrip objects
        from core import CompleteTrip

        trips_data = [
            CompleteTrip(
                trip_key=f"trip-{i}",
                vendor_id=1,
                pickup_ts=datetime(2025, 1, 1, 10, i, 0),
                dropoff_ts=datetime(2025, 1, 1, 10, 30 + i, 0),
                pu_zone_id=1,
                do_zone_id=2,
                passenger_count=1,
                trip_distance=5.5,
                fare_amount=15.50,
                tip_amount=3.00,
                total_amount=18.50,
                payment_type=1,
                vehicle_id_h=f"vehicle-{i}",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            for i in range(3)
        ]

        completed_trips = []

        # Process each trip
        for trip_data in trips_data:
            events = producer.process_trip(trip_data)

            # Process events for this trip
            for event in events:
                complete_trip = assembler.process_event(event.model_dump())
                if complete_trip:
                    completed_trips.append(complete_trip)

        # Should have assembled all trips
        assert len(completed_trips) == 3
        assert all(trip.trip_key.startswith("trip-") for trip in completed_trips)

    @patch("stream.trip_event_producer.KafkaProducer")
    @patch("stream.trip_assembler.KafkaConsumer")
    def test_event_ordering(self, mock_kafka_consumer, mock_kafka_producer):
        """Test that events are processed in correct order."""
        producer = TripEventProducer()
        assembler = TripAssembler()

        from core import CompleteTrip

        trip_data = CompleteTrip(
            trip_key="ordering-test-trip",
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
            vehicle_id_h="vehicle-ordering",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        events = producer.process_trip(trip_data)

        # Process events in reverse order to test robustness
        complete_trip = None
        for event in reversed(events):
            complete_trip = assembler.process_event(event.model_dump())
            if complete_trip:
                break

        # Should still assemble correctly
        assert complete_trip is not None
        assert complete_trip.trip_key == "ordering-test-trip"

    @patch("stream.trip_event_producer.KafkaProducer")
    @patch("stream.trip_assembler.KafkaConsumer")
    def test_partial_trip_cleanup(self, mock_kafka_consumer, mock_kafka_producer):
        """Test that partial trips are cleaned up after completion."""
        producer = TripEventProducer()
        assembler = TripAssembler()

        from core import CompleteTrip

        trip_data = CompleteTrip(
            trip_key="cleanup-test-trip",
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
            vehicle_id_h="vehicle-cleanup",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        events = producer.process_trip(trip_data)

        # Process events until completion
        complete_trip = None
        for event in events:
            complete_trip = assembler.process_event(event.model_dump())
            if complete_trip:
                break

        # Partial trip should be cleaned up
        assert complete_trip is not None
        assert "cleanup-test-trip" not in assembler.partial_trips
        assert len(assembler.completed_trips) == 1
