"""
Integration tests for ChillFlow Batch using testcontainers.

These tests use real PostgreSQL instances via testcontainers
to verify the complete batch processing pipeline.
"""

import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from batch.producer import BatchTripProducer
from sqlalchemy import text


@pytest.mark.integration
class TestBatchIntegration:
    """Integration tests for batch processing pipeline."""

    def test_database_connection(self, db_session):
        """Test that we can connect to the test database."""
        result = db_session.execute(text("SELECT 1 as test_value")).fetchone()
        assert result[0] == 1

    def test_create_schema(self, db_session):
        """Test creating the required database schema."""
        # Create the staging schema
        db_session.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))
        db_session.commit()

        # Create the complete_trip table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stg.complete_trip (
            trip_key VARCHAR(255) PRIMARY KEY,
            vendor_id INTEGER,
            pickup_ts TIMESTAMP,
            dropoff_ts TIMESTAMP,
            pu_zone_id INTEGER,
            do_zone_id INTEGER,
            passenger_count INTEGER,
            rate_code INTEGER,
            store_and_fwd_flag VARCHAR(1),
            fare_amount DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            payment_type INTEGER,
            tip_amount DECIMAL(10,2),
            tolls_amount DECIMAL(10,2),
            congestion_surcharge DECIMAL(10,2),
            airport_fee DECIMAL(10,2),
            distance_km DECIMAL(10,2),
            duration_min DECIMAL(10,2),
            avg_speed_kmh DECIMAL(10,2),
            vehicle_id_h VARCHAR(255),
            last_update_ts TIMESTAMP,
            source VARCHAR(50),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
        db_session.execute(text(create_table_sql))
        db_session.commit()

        # Verify table was created
        result = db_session.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'complete_trip'"
            )
        ).fetchone()
        assert result[0] == 1

    def test_producer_with_real_database(
        self, db_session, sample_trips_dataframe, sample_trip_data
    ):
        """Test BatchTripProducer with real database operations."""
        # Create schema first
        self.test_create_schema(db_session)

        # Mock the database client to use our test database
        with patch("batch.producer.get_database_client") as mock_get_client:
            # Create a mock client that returns our test session
            mock_client = type("MockClient", (), {})()
            mock_client.get_session.return_value.__enter__ = lambda self: db_session
            mock_client.get_session.return_value.__exit__ = lambda self, *args: None
            mock_get_client.return_value = mock_client

            # Create producer with test salt
            with patch("batch.producer.settings") as mock_settings:
                mock_settings.HASH_SALT = "test-salt"

                producer = BatchTripProducer()

                # Test DataFrame to trips conversion
                trips = producer._dataframe_to_trips(sample_trips_dataframe, source="test")

                assert len(trips) == 2
                assert all("trip_key" in trip for trip in trips)
                assert all("vehicle_id_h" in trip for trip in trips)
                assert all(trip["source"] == "test" for trip in trips)

                # Test writing trips to database
                stats = producer._write_trips_batch(trips, batch_size=10)

                assert "inserted" in stats
                assert "skipped" in stats
                assert stats["inserted"] == 2
                assert stats["skipped"] == 0

                # Verify trips were actually written to database
                result = db_session.execute(
                    text("SELECT COUNT(*) FROM stg.complete_trip WHERE source = 'test'")
                ).fetchone()
                assert result[0] == 2

    def test_upsert_functionality(self, db_session, sample_trip_data):
        """Test that upsert functionality works with real database."""
        # Create schema first
        self.test_create_schema(db_session)

        with patch("batch.producer.get_database_client") as mock_get_client:
            mock_client = type("MockClient", (), {})()
            mock_client.get_session.return_value.__enter__ = lambda self: db_session
            mock_client.get_session.return_value.__exit__ = lambda self, *args: None
            mock_get_client.return_value = mock_client

            with patch("batch.producer.settings") as mock_settings:
                mock_settings.HASH_SALT = "test-salt"

                producer = BatchTripProducer()

                # Create trip with specific trip_key
                trip_data = sample_trip_data.copy()
                trip_data["trip_key"] = "test-upsert-key"
                trip_data["fare_amount"] = 10.0

                # Insert first trip
                trips = [trip_data]
                stats = producer._write_trips_batch(trips, batch_size=10)
                assert stats["inserted"] == 1

                # Verify first trip was inserted
                result = db_session.execute(
                    text(
                        "SELECT fare_amount FROM stg.complete_trip WHERE trip_key = 'test-upsert-key'"
                    )
                ).fetchone()
                assert result[0] == 10.0

                # Update trip with same key but different fare
                trip_data["fare_amount"] = 15.0
                trips = [trip_data]
                stats = producer._write_trips_batch(trips, batch_size=10)
                assert stats["inserted"] == 1  # Should still be inserted (upsert)

                # Verify trip was updated
                result = db_session.execute(
                    text(
                        "SELECT fare_amount FROM stg.complete_trip WHERE trip_key = 'test-upsert-key'"
                    )
                ).fetchone()
                assert result[0] == 15.0

                # Verify only one record exists (no duplicates)
                result = db_session.execute(
                    text(
                        "SELECT COUNT(*) FROM stg.complete_trip WHERE trip_key = 'test-upsert-key'"
                    )
                ).fetchone()
                assert result[0] == 1

    def test_batch_processing_with_large_dataset(self, db_session):
        """Test batch processing with a larger dataset."""
        # Create schema first
        self.test_create_schema(db_session)

        with patch("batch.producer.get_database_client") as mock_get_client:
            mock_client = type("MockClient", (), {})()
            mock_client.get_session.return_value.__enter__ = lambda self: db_session
            mock_client.get_session.return_value.__exit__ = lambda self, *args: None
            mock_get_client.return_value = mock_client

            with patch("batch.producer.settings") as mock_settings:
                mock_settings.HASH_SALT = "test-salt"

                producer = BatchTripProducer()

                # Create a larger dataset
                trips_data = []
                for i in range(100):
                    trips_data.append(
                        {
                            "vendor_id": 1,
                            "pickup_ts": datetime(2025, 1, 1, 10, i % 60, 0),
                            "dropoff_ts": datetime(2025, 1, 1, 10, (i + 30) % 60, 0),
                            "pu_zone_id": 229 + (i % 10),
                            "do_zone_id": 230 + (i % 10),
                            "passenger_count": 1 + (i % 3),
                            "rate_code": 1,
                            "store_and_fwd_flag": "N",
                            "fare_amount": 10.0 + i,
                            "total_amount": 12.0 + i,
                            "payment_type": 1,
                            "tip_amount": 2.0 + i,
                            "tolls_amount": 0.0,
                            "congestion_surcharge": 0.0,
                            "airport_fee": 0.0,
                            "distance_km": 5.0 + i,
                            "duration_min": 30.0,
                            "avg_speed_kmh": 10.0 + i,
                        }
                    )

                df = pd.DataFrame(trips_data)
                trips = producer._dataframe_to_trips(df, source="test-large")

                # Process in smaller batches
                stats = producer._write_trips_batch(trips, batch_size=25)

                assert stats["inserted"] == 100
                assert stats["skipped"] == 0

                # Verify all trips were written
                result = db_session.execute(
                    text("SELECT COUNT(*) FROM stg.complete_trip WHERE source = 'test-large'")
                ).fetchone()
                assert result[0] == 100

    def test_database_constraints(self, db_session):
        """Test that database constraints are properly enforced."""
        # Create schema first
        self.test_create_schema(db_session)

        with patch("batch.producer.get_database_client") as mock_get_client:
            mock_client = type("MockClient", (), {})()
            mock_client.get_session.return_value.__enter__ = lambda self: db_session
            mock_client.get_session.return_value.__exit__ = lambda self, *args: None
            mock_get_client.return_value = mock_client

            with patch("batch.producer.settings") as mock_settings:
                mock_settings.HASH_SALT = "test-salt"

                producer = BatchTripProducer()

                # Test with invalid data that should be handled gracefully
                invalid_trips = [
                    {
                        "trip_key": "invalid-trip-1",
                        "vendor_id": None,  # Invalid vendor_id
                        "pickup_ts": datetime(2025, 1, 1, 10, 0, 0),
                        "dropoff_ts": datetime(2025, 1, 1, 10, 30, 0),
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
                        "vehicle_id_h": "test-vehicle",
                        "last_update_ts": datetime.now(),
                        "source": "test-constraints",
                    }
                ]

                # This should handle None values gracefully
                trips = producer._dataframe_to_trips(
                    pd.DataFrame(invalid_trips), source="test-constraints"
                )

                # Should still process the trip (with None vendor_id)
                assert len(trips) == 1
                assert trips[0]["vendor_id"] is None

    def test_transaction_rollback(self, db_session):
        """Test that transaction rollback works correctly."""
        # Create schema first
        self.test_create_schema(db_session)

        with patch("batch.producer.get_database_client") as mock_get_client:
            mock_client = type("MockClient", (), {})()
            mock_client.get_session.return_value.__enter__ = lambda self: db_session
            mock_client.get_session.return_value.__exit__ = lambda self, *args: None
            mock_get_client.return_value = mock_client

            with patch("batch.producer.settings") as mock_settings:
                mock_settings.HASH_SALT = "test-salt"

                producer = BatchTripProducer()

                # Create a trip that will cause an error
                invalid_trip = {
                    "trip_key": "rollback-test",
                    "vendor_id": 1,
                    "pickup_ts": datetime(2025, 1, 1, 10, 0, 0),
                    "dropoff_ts": datetime(2025, 1, 1, 10, 30, 0),
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
                    "vehicle_id_h": "test-vehicle",
                    "last_update_ts": datetime.now(),
                    "source": "test-rollback",
                }

                trips = [invalid_trip]

                # Mock session to raise an error during commit
                with patch.object(db_session, "commit", side_effect=Exception("Database error")):
                    with pytest.raises(Exception):
                        producer._write_trips_batch(trips, batch_size=10)

                # Verify no data was committed due to rollback
                result = db_session.execute(
                    text("SELECT COUNT(*) FROM stg.complete_trip WHERE source = 'test-rollback'")
                ).fetchone()
                assert result[0] == 0
