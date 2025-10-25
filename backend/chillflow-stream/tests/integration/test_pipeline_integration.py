"""
Integration tests for full pipeline.

Tests the complete data flow from Kafka events to database persistence.
"""

from unittest.mock import Mock

import pytest


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for full pipeline."""

    def test_full_pipeline_flow(self, kafka_bootstrap_servers, redis_url, db_connection):
        """Test full pipeline integration: Kafka -> Redis -> DB."""
        from stream.completion_policy import CompletionPolicy
        from stream.kafka_assembly_loop import KafkaAssemblyLoop
        from stream.state_manager import RedisStateManager
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder
        from stream.trip_db_writer import TripDbWriter

        # Setup full pipeline
        state_manager = RedisStateManager(redis_url)
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(state_manager, builder, policy)
        db_writer = TripDbWriter(db_connection)

        # Mock Kafka consumer
        mock_consumer = Mock()

        # Create loop with DB writer as sink
        loop = KafkaAssemblyLoop(mock_consumer, processor, db_writer)

        # Test events
        mock_consumer.__iter__ = Mock(
            return_value=iter(
                [
                    Mock(
                        value={
                            "event_id": "integration-1",
                            "trip_key": "integration-test-123",
                            "event_type": "trip_started",
                            "timestamp": "2024-01-01T12:00:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "pickup_zone_id": 100,
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "integration-2",
                            "trip_key": "integration-test-123",
                            "event_type": "trip_ended",
                            "timestamp": "2024-01-01T12:30:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "dropoff_zone_id": 200,
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "integration-3",
                            "trip_key": "integration-test-123",
                            "event_type": "payment_processed",
                            "timestamp": "2024-01-01T12:31:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "fare_amount": 15.50,
                            "tip_amount": 3.00,
                            "total_amount": 18.50,
                        }
                    ),
                ]
            )
        )

        # Run full pipeline
        result = loop.run(max_records=3, commit_every=2)

        # Should process events and complete trip
        assert result["events"] == 3
        assert result["trips_completed"] == 1

        # Should commit offsets
        assert mock_consumer.commit.call_count == 2

    def test_multiple_trips_pipeline(self, kafka_bootstrap_servers, redis_url, db_connection):
        """Test pipeline with multiple trips."""
        from stream.completion_policy import CompletionPolicy
        from stream.kafka_assembly_loop import KafkaAssemblyLoop
        from stream.state_manager import RedisStateManager
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder
        from stream.trip_db_writer import TripDbWriter

        # Setup full pipeline
        state_manager = RedisStateManager(redis_url)
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(state_manager, builder, policy)
        db_writer = TripDbWriter(db_connection)

        # Mock Kafka consumer
        mock_consumer = Mock()

        # Create loop with DB writer as sink
        loop = KafkaAssemblyLoop(mock_consumer, processor, db_writer)

        # Test events for multiple trips
        mock_consumer.__iter__ = Mock(
            return_value=iter(
                [
                    # Trip 1 - Complete
                    Mock(
                        value={
                            "event_id": "multi-1",
                            "trip_key": "multi-trip-1",
                            "event_type": "trip_started",
                            "timestamp": "2024-01-01T12:00:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "pickup_zone_id": 100,
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "multi-2",
                            "trip_key": "multi-trip-1",
                            "event_type": "trip_ended",
                            "timestamp": "2024-01-01T12:30:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "dropoff_zone_id": 200,
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "multi-3",
                            "trip_key": "multi-trip-1",
                            "event_type": "payment_processed",
                            "timestamp": "2024-01-01T12:31:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "fare_amount": 15.50,
                            "tip_amount": 3.00,
                            "total_amount": 18.50,
                        }
                    ),
                    # Trip 2 - Incomplete (missing payment)
                    Mock(
                        value={
                            "event_id": "multi-4",
                            "trip_key": "multi-trip-2",
                            "event_type": "trip_started",
                            "timestamp": "2024-01-01T13:00:00",
                            "vendor_id": 2,
                            "vehicle_id_h": "vehicle-456",
                            "pickup_zone_id": 300,
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "multi-5",
                            "trip_key": "multi-trip-2",
                            "event_type": "trip_ended",
                            "timestamp": "2024-01-01T13:30:00",
                            "vendor_id": 2,
                            "vehicle_id_h": "vehicle-456",
                            "dropoff_zone_id": 400,
                        }
                    ),
                ]
            )
        )

        # Run full pipeline
        result = loop.run(max_records=5, commit_every=2)

        # Should process 5 events and complete 1 trip
        assert result["events"] == 5
        assert result["trips_completed"] == 1

        # Should commit offsets
        assert mock_consumer.commit.call_count == 3  # After 2nd, 4th, and 5th events

    def test_pipeline_with_errors(self, kafka_bootstrap_servers, redis_url, db_connection):
        """Test pipeline handles errors gracefully."""
        from stream.completion_policy import CompletionPolicy
        from stream.kafka_assembly_loop import KafkaAssemblyLoop
        from stream.state_manager import RedisStateManager
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder
        from stream.trip_db_writer import TripDbWriter

        # Setup full pipeline
        state_manager = RedisStateManager(redis_url)
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(state_manager, builder, policy)
        db_writer = TripDbWriter(db_connection)

        # Mock Kafka consumer
        mock_consumer = Mock()

        # Create loop with DB writer as sink
        loop = KafkaAssemblyLoop(mock_consumer, processor, db_writer)

        # Test events with errors
        mock_consumer.__iter__ = Mock(
            return_value=iter(
                [
                    # Valid event
                    Mock(
                        value={
                            "event_id": "error-1",
                            "trip_key": "error-trip-123",
                            "event_type": "trip_started",
                            "timestamp": "2024-01-01T12:00:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "pickup_zone_id": 100,
                        }
                    ),
                    # Invalid event
                    Mock(
                        value={
                            "event_id": "error-2",
                            "trip_key": "error-trip-123",
                            "event_type": "invalid_event_type",
                            "timestamp": "2024-01-01T12:30:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                        }
                    ),
                    # Valid event
                    Mock(
                        value={
                            "event_id": "error-3",
                            "trip_key": "error-trip-123",
                            "event_type": "trip_ended",
                            "timestamp": "2024-01-01T12:30:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "dropoff_zone_id": 200,
                        }
                    ),
                ]
            )
        )

        # Run full pipeline - should not crash on invalid events
        result = loop.run(max_records=3, commit_every=2)

        # Should process all events but not complete trip (missing payment + invalid event)
        assert result["events"] == 3
        assert result["trips_completed"] == 0

        # Should still commit offsets
        assert mock_consumer.commit.call_count == 2
