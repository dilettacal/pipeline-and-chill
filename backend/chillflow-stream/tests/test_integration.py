"""Test end-to-end stream flow with CLI integration."""

import os
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from kafka import KafkaConsumer, KafkaProducer

from stream.cli import main

from ._kafka_helpers import ensure_topic


@pytest.mark.integration
class TestStreamCLIIntegration:
    """Test CLI integration for stream processing."""

    def test_kafka_roundtrip(self, kafka_bootstrap):
        """Test basic Kafka roundtrip using Testcontainers (expert's approach)."""
        topic = "it-stream-roundtrip"
        partitions = int(os.getenv("KAFKA_IT_TOPIC_PARTITIONS", "1"))
        rf = int(os.getenv("KAFKA_IT_TOPIC_RF", "1"))
        ensure_topic(kafka_bootstrap, topic, partitions, rf)

        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            acks="all",
            retries=5,
            linger_ms=5,
            request_timeout_ms=30000,
        )
        producer.send(topic, b"hello")
        producer.flush()

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=8000,
            session_timeout_ms=15000,
            max_poll_interval_ms=120000,
        )
        msgs = [m.value for m in consumer]
        assert b"hello" in msgs

    @patch("stream.cli.TripEventProducer")
    @patch("stream.cli.get_db_session")
    def test_produce_events_cli(self, mock_get_db_session, mock_producer_class):
        """Test produce-events CLI command."""
        # Mock database session
        mock_session = Mock()
        mock_get_db_session.return_value.__next__.return_value = mock_session

        # Mock database query result
        mock_trip = Mock()
        mock_trip.trip_key = "cli-test-trip"
        mock_trip.vendor_id = 1
        mock_trip.pickup_ts = datetime(2025, 1, 1, 10, 0, 0)
        mock_trip.dropoff_ts = datetime(2025, 1, 1, 10, 30, 0)
        mock_trip.pu_zone_id = 1
        mock_trip.do_zone_id = 2
        mock_trip.passenger_count = 1
        mock_trip.trip_distance = 5.5
        mock_trip.fare_amount = 15.50
        mock_trip.tip_amount = 3.00
        mock_trip.total_amount = 18.50
        mock_trip.payment_type = 1
        mock_trip.vehicle_id_h = "vehicle-cli"
        mock_trip.created_at = datetime.now()
        mock_trip.updated_at = datetime.now()

        # Mock the _asdict method to return a dictionary
        mock_trip._asdict.return_value = {
            "trip_key": "cli-test-trip",
            "vendor_id": 1,
            "pickup_ts": datetime(2025, 1, 1, 10, 0, 0),
            "dropoff_ts": datetime(2025, 1, 1, 10, 30, 0),
            "pu_zone_id": 1,
            "do_zone_id": 2,
            "passenger_count": 1,
            "trip_distance": 5.5,
            "fare_amount": 15.50,
            "tip_amount": 3.00,
            "total_amount": 18.50,
            "payment_type": 1,
            "vehicle_id_h": "vehicle-cli",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }

        mock_session.execute.return_value.fetchall.return_value = [mock_trip]

        # Mock producer
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        mock_producer.process_trips_from_dataframe.return_value = {
            "total_trips": 1,
            "total_events": 5,
            "sent": 5,
            "failed": 0,
        }

        # Test CLI command
        from click.testing import CliRunner

        runner = CliRunner()

        result = runner.invoke(main, ["produce-events", "--limit", "10"])

        if result.exit_code != 0:
            print(f"CLI Error: {result.output}")
            print(f"Exception: {result.exception}")

        assert result.exit_code == 0
        assert "Processed 1 trips" in result.output
        assert "Generated 5 events" in result.output

    @patch("stream.cli.TripAssembler")
    def test_assemble_trips_cli(self, mock_assembler_class):
        """Test assemble-trips CLI command."""
        # Mock assembler
        mock_assembler = Mock()
        mock_assembler_class.return_value = mock_assembler
        mock_assembler.run_assembly_loop.return_value = {
            "trips_assembled": 50,
            "trips_saved": 50,
            "trips_failed": 0,
        }

        # Test CLI command
        from click.testing import CliRunner

        runner = CliRunner()

        result = runner.invoke(main, ["assemble-trips", "--timeout", "10"])

        assert result.exit_code == 0
        assert "Assembled 50 trips" in result.output
        assert "Saved 50 trips to database" in result.output

    @patch("kafka.KafkaConsumer")
    def test_consume_events_cli(self, mock_consumer_class, kafka_bootstrap):
        """Test consume-events CLI command."""
        # Mock consumer
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer

        # Mock event data
        mock_event = {
            "event_type": "trip_started",
            "trip_key": "cli-consume-test",
            "vendor_id": 1,
            "timestamp": "2025-01-01T10:00:00",
            "pickup_zone_id": 1,
            "passenger_count": 1,
            "vehicle_id_h": "vehicle-consume",
        }

        # Mock the consumer to be iterable
        mock_consumer.__iter__ = Mock(return_value=iter([Mock(value=mock_event)]))

        # Test CLI command with environment variable

        from click.testing import CliRunner

        runner = CliRunner()

        # Set the environment variable to use the Testcontainers Kafka
        with runner.isolated_filesystem():
            result = runner.invoke(
                main,
                ["consume-events", "--timeout", "5"],
                env={"KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap},
            )

        assert result.exit_code == 0
        assert "trip_started" in result.output

    @patch("kafka.admin.KafkaAdminClient")
    def test_create_topic_cli(self, mock_admin_class):
        """Test create-topic CLI command."""
        # Mock admin client
        mock_admin = Mock()
        mock_admin_class.return_value = mock_admin

        # Test CLI command
        from click.testing import CliRunner

        runner = CliRunner()

        result = runner.invoke(main, ["create-topic", "--topic", "test-topic"])

        assert result.exit_code == 0
        assert "Created topic 'test-topic'" in result.output
        mock_admin.create_topics.assert_called_once()
        mock_admin.close.assert_called_once()

    def test_cli_help(self):
        """Test CLI help command."""
        from click.testing import CliRunner

        runner = CliRunner()

        result = runner.invoke(main, ["--help"])

        assert result.exit_code == 0
        assert "ChillFlow Stream CLI" in result.output
        assert "produce-events" in result.output
        assert "assemble-trips" in result.output
        assert "consume-events" in result.output
        assert "create-topic" in result.output

    def test_cli_command_help(self):
        """Test individual command help."""
        from click.testing import CliRunner

        runner = CliRunner()

        # Test produce-events help
        result = runner.invoke(main, ["produce-events", "--help"])
        assert result.exit_code == 0
        assert "Produce events from complete trips" in result.output

        # Test assemble-trips help
        result = runner.invoke(main, ["assemble-trips", "--help"])
        assert result.exit_code == 0
        assert "Assemble events into complete trips" in result.output
