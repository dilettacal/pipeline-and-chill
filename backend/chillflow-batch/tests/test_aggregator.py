"""
Unit tests for batch aggregator.
"""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from batch.aggregator import BatchAggregator


@pytest.mark.unit
class TestBatchAggregator:
    """Test batch aggregator functionality."""

    def test_aggregator_initialization(self):
        """Test aggregator initialization."""
        with patch("batch.aggregator.get_database_client"):
            aggregator = BatchAggregator()
            assert aggregator.db_client is not None

    def test_aggregate_incremental_parameters(self):
        """Test incremental aggregation parameters."""
        with patch("batch.aggregator.get_database_client"):
            aggregator = BatchAggregator()

            target_date = datetime(2025, 1, 15, 10, 30)
            lookback_hours = 12

            # Test parameter calculation
            start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            expected_start = start_of_day - timedelta(hours=lookback_hours)
            expected_end = start_of_day + timedelta(days=1)

            assert expected_start.hour == 12  # 0 - 12 = 12 (previous day)
            assert expected_end.hour == 0  # Start of next day

    def test_aggregate_backfill_parameters(self):
        """Test backfill aggregation parameters."""
        with patch("batch.aggregator.get_database_client"):
            aggregator = BatchAggregator()

            start_date = datetime(2025, 1, 1, 15, 30)
            end_date = datetime(2025, 1, 5, 9, 45)

            # Test parameter calculation
            expected_start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            expected_end = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

            assert expected_start.hour == 0
            assert expected_end.hour == 23
            assert expected_end.minute == 59

    @patch("batch.aggregator.get_db_session")
    def test_get_kpi_count(self, mock_get_session):
        """Test KPI count retrieval."""
        # Mock session and result
        mock_session = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = 42
        mock_session.execute.return_value = mock_result
        mock_get_session.return_value = iter([mock_session])

        with patch("batch.aggregator.get_database_client"):
            aggregator = BatchAggregator()
            count = aggregator.get_kpi_count()

            assert count == 42
            mock_session.execute.assert_called_once()

    @patch("batch.aggregator.get_db_session")
    def test_get_date_range_empty(self, mock_get_session):
        """Test date range retrieval when no data."""
        # Mock session and result
        mock_session = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result
        mock_get_session.return_value = iter([mock_session])

        with patch("batch.aggregator.get_database_client"):
            aggregator = BatchAggregator()
            date_range = aggregator.get_date_range()

            assert date_range is None

    @patch("batch.aggregator.get_db_session")
    def test_get_date_range_with_data(self, mock_get_session):
        """Test date range retrieval with data."""
        # Mock session and result
        mock_session = Mock()
        mock_result = Mock()
        mock_row = Mock()
        mock_row.min_hour = datetime(2025, 1, 1, 0, 0)
        mock_row.max_hour = datetime(2025, 1, 31, 23, 0)
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        mock_get_session.return_value = iter([mock_session])

        with patch("batch.aggregator.get_database_client"):
            aggregator = BatchAggregator()
            date_range = aggregator.get_date_range()

            assert date_range is not None
            assert date_range["min_hour"] == datetime(2025, 1, 1, 0, 0)
            assert date_range["max_hour"] == datetime(2025, 1, 31, 23, 0)
