"""
Unit tests for batch trip producer.
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from batch.producer import BatchTripProducer


@pytest.mark.unit
class TestBatchTripProducer:
    """Test batch trip producer functionality."""

    def test_producer_initialization(self):
        """Test producer initialization."""
        with (
            patch("batch.producer.get_database_client"),
            patch("batch.producer.settings") as mock_settings,
        ):
            mock_settings.HASH_SALT = "test-salt"

            producer = BatchTripProducer()
            assert producer.db_client is not None
            assert producer.hash_salt == "test-salt"

    def test_dataframe_to_trips(self):
        """Test DataFrame to trips conversion."""
        with (
            patch("batch.producer.get_database_client"),
            patch("batch.producer.settings") as mock_settings,
        ):
            mock_settings.HASH_SALT = "test-salt"

            producer = BatchTripProducer()

            # Create test DataFrame
            df = pd.DataFrame(
                {
                    "vendor_id": [1, 2],
                    "pickup_ts": [
                        datetime(2025, 1, 1, 10, 0),
                        datetime(2025, 1, 1, 11, 0),
                    ],
                    "dropoff_ts": [
                        datetime(2025, 1, 1, 10, 30),
                        datetime(2025, 1, 1, 11, 30),
                    ],
                    "pu_zone_id": [229, 230],
                    "do_zone_id": [230, 231],
                    "passenger_count": [1, 2],
                    "rate_code": [1, 1],
                    "store_and_fwd_flag": ["N", "N"],
                    "fare_amount": [10.0, 15.0],
                    "total_amount": [12.0, 18.0],
                    "payment_type": [1, 1],
                    "tip_amount": [2.0, 3.0],
                    "tolls_amount": [0.0, 0.0],
                    "congestion_surcharge": [0.0, 0.0],
                    "airport_fee": [0.0, 0.0],
                    "distance_km": [5.0, 8.0],
                    "duration_min": [30.0, 30.0],
                    "avg_speed_kmh": [10.0, 16.0],
                }
            )

            trips = producer._dataframe_to_trips(df, source="test")

            assert len(trips) == 2
            assert all("trip_key" in trip for trip in trips)
            assert all("vehicle_id_h" in trip for trip in trips)
            assert all(trip["source"] == "test" for trip in trips)
            assert all(trip["vendor_id"] in [1, 2] for trip in trips)

    def test_dataframe_to_trips_with_nulls(self):
        """Test DataFrame to trips conversion with null values."""
        with (
            patch("batch.producer.get_database_client"),
            patch("batch.producer.settings") as mock_settings,
        ):
            mock_settings.HASH_SALT = "test-salt"

            producer = BatchTripProducer()

            # Create test DataFrame with nulls
            df = pd.DataFrame(
                {
                    "vendor_id": [1],
                    "pickup_ts": [datetime(2025, 1, 1, 10, 0)],
                    "dropoff_ts": [datetime(2025, 1, 1, 10, 30)],
                    "pu_zone_id": [229],
                    "do_zone_id": [pd.NA],  # Null value
                    "passenger_count": [pd.NA],  # Null value
                    "rate_code": [1],
                    "store_and_fwd_flag": ["N"],
                    "fare_amount": [10.0],
                    "total_amount": [12.0],
                    "payment_type": [1],
                    "tip_amount": [2.0],
                    "tolls_amount": [0.0],
                    "congestion_surcharge": [0.0],
                    "airport_fee": [0.0],
                    "distance_km": [5.0],
                    "duration_min": [30.0],
                    "avg_speed_kmh": [10.0],
                }
            )

            trips = producer._dataframe_to_trips(df, source="test")

            assert len(trips) == 1
            trip = trips[0]
            assert trip["do_zone_id"] is None
            assert trip["passenger_count"] is None

    @patch("batch.producer.get_db_session")
    def test_write_trips_batch(self, mock_get_session):
        """Test batch writing of trips."""
        # Mock session
        mock_session = Mock()
        mock_get_session.return_value = iter([mock_session])

        with (
            patch("batch.producer.get_database_client"),
            patch("batch.producer.settings") as mock_settings,
        ):
            mock_settings.HASH_SALT = "test-salt"

            producer = BatchTripProducer()

            # Create test trips
            trips = [
                {
                    "trip_key": "test_key_1",
                    "vendor_id": 1,
                    "vehicle_id_h": "test_vehicle_1",
                    "pickup_ts": datetime(2025, 1, 1, 10, 0),
                    "dropoff_ts": datetime(2025, 1, 1, 10, 30),
                    "pu_zone_id": 229,
                    "do_zone_id": 230,
                    "passenger_count": 1,
                    "rate_code": 1,
                    "store_and_fwd_flag": "N",
                    "fare_amount": 10.0,
                    "total_amount": 12.0,
                    "payment_type": 1,
                    "tip_amount": 2.0,
                    "tolls_amount": 0.0,
                    "congestion_surcharge": 0.0,
                    "airport_fee": 0.0,
                    "distance_km": 5.0,
                    "duration_min": 30.0,
                    "avg_speed_kmh": 10.0,
                    "last_update_ts": datetime.now(),
                    "source": "test",
                }
            ]

            stats = producer._write_trips_batch(trips, batch_size=1)

            assert "inserted" in stats
            assert "skipped" in stats
            mock_session.commit.assert_called_once()

    def test_process_month_file_not_found(self):
        """Test processing non-existent file."""
        with (
            patch("batch.producer.get_database_client"),
            patch("batch.producer.settings") as mock_settings,
        ):
            mock_settings.HASH_SALT = "test-salt"

            producer = BatchTripProducer()

            non_existent_path = Path("/non/existent/file.parquet")

            with pytest.raises(FileNotFoundError):
                producer.process_month(non_existent_path)
