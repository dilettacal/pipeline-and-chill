"""
Integration tests for KafkaAssemblyLoop.

Tests the orchestration logic with real dependencies using Testcontainers.
"""

from unittest.mock import Mock

import pytest


@pytest.mark.integration
class TestKafkaLoop:
    """Integration tests for KafkaAssemblyLoop."""

    def test_processes_events_and_commits_offsets(self, kafka_bootstrap_servers, redis_url):
        """Test KafkaAssemblyLoop processes events and commits offsets."""
        from stream.completion_policy import CompletionPolicy
        from stream.kafka_assembly_loop import KafkaAssemblyLoop
        from stream.state_manager import RedisStateManager
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Setup components
        state_manager = RedisStateManager(redis_url)
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(state_manager, builder, policy)

        # Mock Kafka consumer and producer
        mock_consumer = Mock()
        mock_sink = Mock()

        # Create loop
        loop = KafkaAssemblyLoop(mock_consumer, processor, mock_sink)

        # Test processing
        mock_consumer.__iter__ = Mock(
            return_value=iter(
                [
                    Mock(
                        value={
                            "event_id": "test-1",
                            "trip_key": "loop-test-123",
                            "event_type": "trip_started",
                            "timestamp": "2024-01-01T12:00:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "pickup_zone_id": 100,
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "test-2",
                            "trip_key": "loop-test-123",
                            "event_type": "trip_ended",
                            "timestamp": "2024-01-01T12:30:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "dropoff_zone_id": 200,
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "test-3",
                            "trip_key": "loop-test-123",
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

        # Run loop with max_records=3
        result = loop.run(max_records=3, commit_every=2)

        # Should process 3 events and complete 1 trip
        assert result["events"] == 3
        assert result["trips_completed"] == 1

        # Should commit offsets (2 commits: after 2nd and 3rd events)
        assert mock_consumer.commit.call_count == 2

        # Should write complete trip to sink
        assert mock_sink.write.call_count == 1
        written_trip = mock_sink.write.call_args[0][0]
        assert written_trip.trip_key == "loop-test-123"
        assert written_trip.fare_amount == 15.50

    def test_handles_processing_errors(self, kafka_bootstrap_servers, redis_url):
        """Test KafkaAssemblyLoop handles processing errors gracefully."""
        from stream.completion_policy import CompletionPolicy
        from stream.kafka_assembly_loop import KafkaAssemblyLoop
        from stream.state_manager import RedisStateManager
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Setup components
        state_manager = RedisStateManager(redis_url)
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(state_manager, builder, policy)

        # Mock Kafka consumer and producer
        mock_consumer = Mock()
        mock_sink = Mock()

        # Create loop
        loop = KafkaAssemblyLoop(mock_consumer, processor, mock_sink)

        # Test with mixed valid and invalid events
        mock_consumer.__iter__ = Mock(
            return_value=iter(
                [
                    Mock(
                        value={
                            "event_id": "test-1",
                            "trip_key": "error-test-123",
                            "event_type": "trip_started",
                            "timestamp": "2024-01-01T12:00:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "pickup_zone_id": 100,
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "test-2",
                            "trip_key": "error-test-123",
                            "event_type": "invalid_event_type",  # Invalid event
                            "timestamp": "2024-01-01T12:30:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                        }
                    ),
                    Mock(
                        value={
                            "event_id": "test-3",
                            "trip_key": "error-test-123",
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

        # Run loop - should not crash on invalid events
        result = loop.run(max_records=3, commit_every=2)

        # Should process all events but not complete trip (missing payment)
        assert result["events"] == 3
        assert result["trips_completed"] == 0

        # Should still commit offsets
        assert mock_consumer.commit.call_count == 2

        # Should not write to sink (no complete trips)
        assert mock_sink.write.call_count == 0

    def test_respects_max_records_limit(self, kafka_bootstrap_servers, redis_url):
        """Test KafkaAssemblyLoop respects max_records limit."""
        from stream.completion_policy import CompletionPolicy
        from stream.kafka_assembly_loop import KafkaAssemblyLoop
        from stream.state_manager import RedisStateManager
        from stream.trip_assembler_processor import TripAssemblerProcessor
        from stream.trip_builder import TripBuilder

        # Setup components
        state_manager = RedisStateManager(redis_url)
        builder = TripBuilder()
        policy = CompletionPolicy()
        processor = TripAssemblerProcessor(state_manager, builder, policy)

        # Mock Kafka consumer and producer
        mock_consumer = Mock()
        mock_sink = Mock()

        # Create loop
        loop = KafkaAssemblyLoop(mock_consumer, processor, mock_sink)

        # Test with more events than limit
        mock_consumer.__iter__ = Mock(
            return_value=iter(
                [
                    Mock(
                        value={
                            "event_id": f"test-{i}",
                            "trip_key": f"limit-test-{i}",
                            "event_type": "trip_started",
                            "timestamp": "2024-01-01T12:00:00",
                            "vendor_id": 1,
                            "vehicle_id_h": "vehicle-123",
                            "pickup_zone_id": 100,
                        }
                    )
                    for i in range(10)  # 10 events
                ]
            )
        )

        # Run loop with max_records=5
        result = loop.run(max_records=5, commit_every=2)

        # Should only process 5 events
        assert result["events"] == 5
        assert result["trips_completed"] == 0  # No complete trips

        # Should commit offsets
        assert mock_consumer.commit.call_count == 3  # After 2nd, 4th, and 5th events
