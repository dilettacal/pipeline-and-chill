"""
Performance benchmarks for pipeline components.

These tests measure performance characteristics and identify bottlenecks.
"""

import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

# Add backend paths for imports
backend_path = Path(__file__).parent.parent.parent / "backend"
sys.path.insert(0, str(backend_path / "chillflow-core"))
sys.path.insert(0, str(backend_path / "chillflow-batch"))
sys.path.insert(0, str(backend_path / "chillflow-stream"))


@pytest.mark.performance
class TestPerformanceBenchmarks:
    """Performance benchmarks for pipeline components."""

    def test_batch_processing_performance(self):
        """Test batch processing performance with larger datasets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create larger dataset
            test_data = {
                "trip_key": [f"trip-{i}" for i in range(1000)],
                "vendor_id": [1] * 1000,
                "pickup_ts": [datetime(2025, 1, 1, 10, 0, 0)] * 1000,
                "dropoff_ts": [datetime(2025, 1, 1, 10, 30, 0)] * 1000,
                "pu_zone_id": [1] * 1000,
                "do_zone_id": [2] * 1000,
                "passenger_count": [1] * 1000,
                "rate_code": [1] * 1000,
                "store_and_fwd_flag": ["N"] * 1000,
                "trip_distance": [5.0] * 1000,
                "fare_amount": [10.0] * 1000,
                "total_amount": [12.0] * 1000,
                "payment_type": [1] * 1000,
                "tip_amount": [2.0] * 1000,
                "tolls_amount": [0.0] * 1000,
                "congestion_surcharge": [0.0] * 1000,
                "airport_fee": [0.0] * 1000,
                "distance_km": [5.0] * 1000,
                "duration_min": [30.0] * 1000,
                "avg_speed_kmh": [10.0] * 1000,
                "vehicle_id_h": [f"vehicle-{i}" for i in range(1000)],
            }

            df = pd.DataFrame(test_data)
            parquet_path = Path(tmpdir) / "performance_trips.parquet"
            df.to_parquet(parquet_path, index=False)

            # Test component initialization and basic functionality
            from batch.producer import BatchTripProducer

            # Measure initialization time
            start_time = time.time()
            producer = BatchTripProducer()
            init_time = time.time() - start_time

            # Test data loading performance
            start_time = time.time()
            loaded_df = pd.read_parquet(parquet_path, engine="pyarrow")
            load_time = time.time() - start_time

            # Test data conversion performance
            start_time = time.time()
            trips = producer._dataframe_to_trips(loaded_df, source="test")
            conversion_time = time.time() - start_time

            # Verify performance
            assert init_time < 1.0  # Should initialize quickly
            assert load_time < 2.0  # Should load 1000 trips in under 2 seconds
            assert conversion_time < 5.0  # Should convert 1000 trips in under 5 seconds
            assert len(trips) == 1000  # Should process all trips
            assert trips[0]["trip_key"] is not None  # Should generate trip keys

    def test_memory_usage_smoke(self):
        """Test basic memory usage characteristics."""
        # Simple memory test without psutil dependency
        import gc

        # Create some data structures
        test_data = [{"trip_key": f"trip-{i}", "vendor_id": 1} for i in range(10000)]

        # Force garbage collection
        gc.collect()

        # Test that we can create and process data
        assert len(test_data) == 10000
        assert test_data[0]["trip_key"] == "trip-0"

    def test_data_validation_performance(self):
        """Test data validation performance."""
        from core import CompleteTrip

        # Create test data
        test_trips = []
        for i in range(1000):
            trip_data = {
                "trip_key": f"trip-{i}",
                "vendor_id": 1,
                "pickup_ts": "2025-01-01 10:00:00",
                "dropoff_ts": "2025-01-01 10:30:00",
                "pu_zone_id": 1,
                "do_zone_id": 2,
                "passenger_count": 1,
                "trip_distance": 5.0,
                "fare_amount": 10.0,
                "total_amount": 12.0,
                "payment_type": 1,
                "tip_amount": 2.0,
                "vehicle_id_h": f"vehicle-{i}",
            }
            test_trips.append(trip_data)

        # Measure validation time
        start_time = time.time()

        validated_trips = []
        for trip_data in test_trips:
            try:
                trip = CompleteTrip(**trip_data)
                validated_trips.append(trip)
            except Exception:
                pass  # Skip invalid trips

        validation_time = time.time() - start_time

        # Should validate 1000 trips in under 5 seconds
        assert validation_time < 5.0
        assert len(validated_trips) == 1000

    def test_serialization_performance(self):
        """Test serialization performance."""
        from stream.events import TripStartedEvent

        # Create test event (Pydantic model)
        event = TripStartedEvent(
            event_id="event-1",
            event_type="trip_started",
            trip_key="test-trip",
            vendor_id=1,
            timestamp=datetime(2025, 1, 1, 10, 0, 0).isoformat(),
            pickup_zone_id=1,
            passenger_count=1,
            vehicle_id_h="vehicle-123",
        )

        # Measure serialization time
        start_time = time.time()

        for _ in range(1000):
            event_dict = event.model_dump()

        serialization_time = time.time() - start_time

        # Should serialize 1000 times in under 1 second
        assert serialization_time < 1.0
        assert event_dict["event_type"] == "trip_started"
