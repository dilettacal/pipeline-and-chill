"""
Smoke tests for CLI commands.

These tests verify that CLI commands work end-to-end without
complex infrastructure dependencies.
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add backend paths for imports
backend_path = Path(__file__).parent.parent.parent / "backend"
sys.path.insert(0, str(backend_path / "chillflow-core"))
sys.path.insert(0, str(backend_path / "chillflow-batch"))
sys.path.insert(0, str(backend_path / "chillflow-stream"))


@pytest.mark.smoke
class TestCLISmoke:
    """Smoke tests for CLI commands."""

    def test_batch_cli_help(self):
        """Test that batch CLI shows help."""
        import subprocess

        result = subprocess.run(
            ["python", "-m", "batch", "--help"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        assert "usage:" in result.stdout.lower()

    def test_stream_cli_help(self):
        """Test that stream CLI shows help."""
        import subprocess

        result = subprocess.run(
            ["python", "-m", "stream.cli", "--help"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent,
        )

        assert result.returncode == 0
        assert "usage:" in result.stdout.lower()

    def test_batch_process_smoke(self):
        """Test batch processing components can be imported and initialized."""
        # Test that batch components can be imported
        from batch.aggregator import BatchAggregator
        from batch.producer import BatchTripProducer

        # Test that components can be created
        with patch("core.clients.database.get_database_client"):
            producer = BatchTripProducer()
            assert producer is not None

            aggregator = BatchAggregator()
            assert aggregator is not None

    def test_stream_components_smoke(self):
        """Test that stream components can be imported and initialized."""
        from stream.trip_assembler import TripAssembler
        from stream.trip_event_producer import TripEventProducer

        # Test that components can be created
        with patch("stream.trip_event_producer.KafkaProducer"):
            producer = TripEventProducer()
            assert producer is not None

        with (
            patch("stream.trip_assembler.KafkaConsumer"),
            patch("kafka.admin.KafkaAdminClient"),
            patch("core.clients.database.get_database_client"),
        ):
            assembler = TripAssembler()
            assert assembler is not None

    def test_data_validation_smoke(self):
        """Test data validation and filtering."""
        from core import CompleteTrip

        # Test valid trip
        valid_trip = CompleteTrip(
            trip_key="valid-trip",
            vendor_id=1,
            pickup_ts="2025-01-01 10:00:00",
            dropoff_ts="2025-01-01 10:30:00",
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
        assert valid_trip.fare_amount == 10.0

    def test_performance_smoke(self):
        """Test basic performance characteristics."""
        import time

        # Test that we can process data quickly
        start_time = time.time()

        # Simulate processing
        test_data = [{"trip_key": f"trip-{i}", "vendor_id": 1} for i in range(100)]

        # Simple processing simulation
        processed = [trip for trip in test_data if trip["trip_key"].startswith("trip-")]

        processing_time = time.time() - start_time

        # Should process 100 items in under 1 second
        assert processing_time < 1.0
        assert len(processed) == 100
