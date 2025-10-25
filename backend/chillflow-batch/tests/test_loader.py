"""
Unit tests for monthly loader.
"""

from datetime import datetime

import pandas as pd
import pytest
from batch.loader import (
    MonthlyLoader,
    apply_dq_rules,
    compute_derived_fields,
    standardize_column_names,
)


@pytest.mark.unit
class TestMonthlyLoader:
    """Test monthly loader functionality."""

    def test_compute_derived_fields(self):
        """Test derived fields computation."""
        # Create test DataFrame
        df = pd.DataFrame(
            {
                "trip_distance": [1.0, 2.0, 3.0],
                "tpep_pickup_datetime": [
                    datetime(2025, 1, 1, 10, 0),
                    datetime(2025, 1, 1, 11, 0),
                    datetime(2025, 1, 1, 12, 0),
                ],
                "tpep_dropoff_datetime": [
                    datetime(2025, 1, 1, 10, 30),
                    datetime(2025, 1, 1, 11, 30),
                    datetime(2025, 1, 1, 12, 30),
                ],
                "total_amount": [10.0, 20.0, 30.0],
            }
        )

        result_df = compute_derived_fields(df)

        # Check distance conversion (miles to km)
        assert "distance_km" in result_df.columns
        assert result_df["distance_km"].iloc[0] == pytest.approx(1.60934, rel=1e-5)

        # Check duration calculation
        assert "duration_min" in result_df.columns
        assert result_df["duration_min"].iloc[0] == pytest.approx(30.0, rel=1e-5)

        # Check speed calculation
        assert "avg_speed_kmh" in result_df.columns
        assert result_df["avg_speed_kmh"].iloc[0] == pytest.approx(3.21868, rel=1e-5)

    def test_apply_dq_rules(self):
        """Test data quality rules application."""
        # Create test DataFrame with various issues
        df = pd.DataFrame(
            {
                "tpep_pickup_datetime": [
                    datetime(2025, 1, 1, 10, 0),
                    datetime(2025, 1, 1, 11, 0),
                    datetime(2025, 1, 1, 12, 0),
                    datetime(2025, 1, 1, 13, 0),
                ],
                "tpep_dropoff_datetime": [
                    datetime(2025, 1, 1, 10, 30),  # Valid
                    datetime(2025, 1, 1, 10, 30),  # Invalid: pickup >= dropoff
                    datetime(2025, 1, 1, 12, 30),  # Valid
                    datetime(2025, 1, 1, 13, 30),  # Valid
                ],
                "avg_speed_kmh": [50.0, 100.0, 250.0, 75.0],  # 250.0 should be filtered
                "total_amount": [10.0, 20.0, 30.0, -5.0],  # -5.0 should be filtered
            }
        )

        result_df, rejections = apply_dq_rules(df)

        # Check rejections
        assert rejections["temporal_invalid"] == 1
        assert rejections["speed_invalid"] == 1
        assert rejections["negative_amount"] == 1

        # Check final count (should be 1 valid row)
        assert len(result_df) == 1
        assert result_df["total_amount"].iloc[0] == 10.0

    def test_standardize_column_names(self):
        """Test column name standardization."""
        # Create test DataFrame with TLC column names
        df = pd.DataFrame(
            {
                "VendorID": [1, 2],
                "tpep_pickup_datetime": [
                    datetime(2025, 1, 1, 10, 0),
                    datetime(2025, 1, 1, 11, 0),
                ],
                "tpep_dropoff_datetime": [
                    datetime(2025, 1, 1, 10, 30),
                    datetime(2025, 1, 1, 11, 30),
                ],
                "PULocationID": [229, 230],
                "DOLocationID": [230, 231],
                "RatecodeID": [1, 1],
                "Airport_fee": [0.0, 0.0],
                "passenger_count": [1, 2],  # Already snake_case
                "fare_amount": [10.0, 15.0],  # Already snake_case
            }
        )

        result_df = standardize_column_names(df)

        # Check renamed columns
        assert "vendor_id" in result_df.columns
        assert "pickup_ts" in result_df.columns
        assert "dropoff_ts" in result_df.columns
        assert "pu_zone_id" in result_df.columns
        assert "do_zone_id" in result_df.columns
        assert "rate_code" in result_df.columns
        assert "airport_fee" in result_df.columns

        # Check preserved columns
        assert "passenger_count" in result_df.columns
        assert "fare_amount" in result_df.columns

    def test_loader_initialization(self):
        """Test loader initialization."""
        loader = MonthlyLoader()
        assert loader is not None

    @pytest.mark.skip(reason="Requires file I/O")
    def test_curate_month_integration(self):
        """Test full month curation pipeline."""
        # This would require actual file I/O, so we'll skip it in unit tests
        # Integration tests would cover this
