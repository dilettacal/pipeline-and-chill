"""
Integration tests for ChillFlow Stream using testcontainers.

These tests use real Kafka and PostgreSQL instances via testcontainers
to verify the complete stream processing pipeline.
"""

import json
import time
from datetime import datetime

import pytest
from kafka import KafkaConsumer, KafkaProducer

from stream.trip_assembler import TripAssembler
from stream.trip_event_producer import TripEventProducer


@pytest.mark.integration
class TestStreamIntegration:
    """Integration tests for stream processing pipeline."""

    def test_producer_to_assembler_flow(
        self, kafka_producer, kafka_consumer, kafka_topic, sample_complete_trip
    ):
        """Test complete flow from producer to assembler using real Kafka."""
        # Create producer with real Kafka connection
        producer = TripEventProducer(
            kafka_bootstrap_servers=kafka_producer.config["bootstrap_servers"][0]
        )

        # Create assembler with real Kafka connection
        assembler = TripAssembler(
            kafka_bootstrap_servers=kafka_consumer.config["bootstrap_servers"][0], topic=kafka_topic
        )

        # Generate events from complete trip
        events = producer.process_trip(sample_complete_trip)
        assert len(events) == 5  # Should generate 5 events

        # Send events to Kafka
        for event in events:
            kafka_producer.send(kafka_topic, key=event.trip_key, value=event.model_dump())
        kafka_producer.flush()

        # Consume events and assemble trip
        complete_trip = None
        for message in kafka_consumer:
            event_data = message.value
            complete_trip = assembler.process_event(event_data)
            if complete_trip:
                break

        # Verify trip was assembled
        assert complete_trip is not None
        assert complete_trip.trip_key == sample_complete_trip.trip_key
        assert complete_trip.vendor_id == sample_complete_trip.vendor_id
        assert complete_trip.fare_amount == sample_complete_trip.fare_amount

    def test_kafka_message_roundtrip(
        self, kafka_producer, kafka_consumer, kafka_topic, sample_events
    ):
        """Test Kafka message roundtrip with real broker."""
        # Send events to Kafka
        for event in sample_events:
            kafka_producer.send(kafka_topic, key=event["trip_key"], value=event)
        kafka_producer.flush()

        # Consume events
        received_events = []
        for message in kafka_consumer:
            received_events.append(message.value)
            if len(received_events) >= len(sample_events):
                break

        # Verify all events received
        assert len(received_events) == len(sample_events)

        # Verify event content
        for i, event in enumerate(sample_events):
            received = received_events[i]
            assert received["trip_key"] == event["trip_key"]
            assert received["event_type"] == event["event_type"]

    def test_kafka_message_ordering(
        self, kafka_producer, kafka_consumer, kafka_topic, sample_events
    ):
        """Test that messages maintain order within partition."""
        trip_key = sample_events[0]["trip_key"]

        # Send events in order
        for i, event in enumerate(sample_events):
            event["sequence"] = i
            kafka_producer.send(kafka_topic, key=trip_key, value=event)
        kafka_producer.flush()

        # Consume events
        received_sequences = []
        for message in kafka_consumer:
            if message.key == trip_key:
                received_sequences.append(message.value["sequence"])
                if len(received_sequences) >= len(sample_events):
                    break

        # Verify order is maintained
        assert received_sequences == list(range(len(sample_events)))

    def test_kafka_json_serialization(self, kafka_producer, kafka_consumer, kafka_topic):
        """Test that complex JSON objects are serialized/deserialized correctly."""
        trip_key = f"test_json_{datetime.now().timestamp()}"

        event = {
            "event_id": f"event-{trip_key}",
            "event_type": "trip_started",
            "trip_key": trip_key,
            "vendor_id": 1,
            "timestamp": datetime.now().isoformat(),
            "nested": {
                "field1": "value1",
                "field2": 123,
                "field3": [1, 2, 3],
            },
            "unicode": "Hello 世界 🚕",
        }

        # Send event
        kafka_producer.send(kafka_topic, key=trip_key, value=event)
        kafka_producer.flush()

        # Consume event
        for message in kafka_consumer:
            if message.key == trip_key:
                received = message.value

                # Verify all fields preserved
                assert received["nested"]["field1"] == "value1"
                assert received["nested"]["field2"] == 123
                assert received["nested"]["field3"] == [1, 2, 3]
                assert received["unicode"] == "Hello 世界 🚕"
                break

    def test_kafka_high_throughput(self, kafka_producer, kafka_consumer, kafka_topic):
        """Test handling of high message throughput."""
        num_messages = 100
        start_time = time.time()

        # Send many messages
        for i in range(num_messages):
            event = {
                "event_id": f"event-{i}",
                "event_type": "trip_started",
                "trip_key": f"test_throughput_{i}",
                "vendor_id": 1,
                "timestamp": datetime.now().isoformat(),
            }
            kafka_producer.send(kafka_topic, key=event["trip_key"], value=event)

        kafka_producer.flush()
        send_duration = time.time() - start_time

        # Consume messages
        start_time = time.time()
        received_count = 0
        for message in kafka_consumer:
            received_count += 1
            if received_count >= num_messages:
                break

        consume_duration = time.time() - start_time

        # Verify all messages received
        assert received_count == num_messages

        # Log performance metrics
        print(f"\nThroughput test:")
        print(
            f"  Sent {num_messages} messages in {send_duration:.2f}s ({num_messages/send_duration:.0f} msg/s)"
        )
        print(
            f"  Consumed {num_messages} messages in {consume_duration:.2f}s ({num_messages/consume_duration:.0f} msg/s)"
        )

    def test_assembler_with_real_kafka(
        self, kafka_producer, kafka_consumer, kafka_topic, sample_events
    ):
        """Test TripAssembler with real Kafka connection."""
        # Create assembler with real Kafka connection
        assembler = TripAssembler(
            kafka_bootstrap_servers=kafka_consumer.config["bootstrap_servers"][0], topic=kafka_topic
        )

        # Send events to Kafka
        for event in sample_events:
            kafka_producer.send(kafka_topic, key=event["trip_key"], value=event)
        kafka_producer.flush()

        # Process events through assembler
        complete_trip = None
        for message in kafka_consumer:
            event_data = message.value
            complete_trip = assembler.process_event(event_data)
            if complete_trip:
                break

        # Verify trip was assembled
        assert complete_trip is not None
        assert complete_trip.trip_key == sample_events[0]["trip_key"]

        # Cleanup
        assembler.close()

    def test_producer_with_real_kafka(
        self, kafka_producer, kafka_consumer, kafka_topic, sample_complete_trip
    ):
        """Test TripEventProducer with real Kafka connection."""
        # Create producer with real Kafka connection
        producer = TripEventProducer(
            kafka_bootstrap_servers=kafka_producer.config["bootstrap_servers"][0]
        )

        # Generate events
        events = producer.process_trip(sample_complete_trip)
        assert len(events) == 5

        # Send events to Kafka
        for event in events:
            kafka_producer.send(kafka_topic, key=event.trip_key, value=event.model_dump())
        kafka_producer.flush()

        # Consume events
        received_events = []
        for message in kafka_consumer:
            received_events.append(message.value)
            if len(received_events) >= len(events):
                break

        # Verify events received
        assert len(received_events) == len(events)

        # Cleanup
        producer.close()
