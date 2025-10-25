"""
Test for MetricsCollector to verify metrics are being recorded properly.
"""

import pytest
import requests
from core.metrics import MetricsCollector, get_metrics_response


class TestMetricsCollector:
    """Test the MetricsCollector functionality."""

    def test_metrics_collector_basic_functionality(self):
        """Test basic MetricsCollector functionality."""
        # Create a metrics collector
        metrics = MetricsCollector("test-service")

        # Record some test metrics
        metrics.record_trip_event("trip_started")
        metrics.record_trip_event("trip_ended")
        metrics.record_trip_event("zone_entered")
        metrics.record_trip_event("zone_exited")
        metrics.record_trip_event("payment_processed")

        # Test trip completion
        metrics.record_trip_completed()
        metrics.record_trip_completed()

        # Test processing time
        metrics.record_processing_time("test_operation", 1.5)
        metrics.record_processing_time("batch_processing", 2.3)

        # Test batch size
        metrics.record_batch_size(100)
        metrics.record_batch_size(250)

        # Test Redis operations
        metrics.record_redis_operation("set", "success")
        metrics.record_redis_operation("get", "success")
        metrics.record_redis_operation("hset", "success")

        # Test data quality failures
        metrics.record_dq_failure("schema_validation")
        metrics.record_dq_failure("missing_field")

        # Get metrics response
        metrics_data, content_type = get_metrics_response()

        # Verify response
        assert content_type == "text/plain; version=1.0.0; charset=utf-8"
        assert len(metrics_data) > 0

        # Decode and check for our metrics
        metrics_text = metrics_data.decode("utf-8")

        # Check for specific metrics
        assert "trip_events_total" in metrics_text
        assert "trips_completed_total" in metrics_text
        assert "processing_latency_seconds" in metrics_text
        assert "batch_size" in metrics_text
        assert "redis_operations_total" in metrics_text
        assert "dq_failures_total" in metrics_text

        # Verify specific values
        assert (
            'trip_events_total{event_type="trip_started",service="test-service"} 1.0'
            in metrics_text
        )
        assert 'trips_completed_total{service="test-service"} 2.0' in metrics_text

    def test_metrics_collector_different_services(self):
        """Test that different services have separate metrics."""
        # Create two different service collectors
        metrics1 = MetricsCollector("service-1")
        metrics2 = MetricsCollector("service-2")

        # Record metrics for each service
        metrics1.record_trip_event("trip_started")
        metrics2.record_trip_event("trip_ended")

        # Get combined metrics
        metrics_data, _ = get_metrics_response()
        metrics_text = metrics_data.decode("utf-8")

        # Verify both services are present
        assert 'service="service-1"' in metrics_text
        assert 'service="service-2"' in metrics_text
        assert (
            'trip_events_total{event_type="trip_started",service="service-1"} 1.0' in metrics_text
        )
        assert 'trip_events_total{event_type="trip_ended",service="service-2"} 1.0' in metrics_text

    def test_metrics_collector_error_handling(self):
        """Test error handling in metrics collection."""
        metrics = MetricsCollector("error-test")

        # Test with invalid inputs (should not crash)
        try:
            metrics.record_processing_time("test", -1.0)  # Negative time
            metrics.record_batch_size(-10)  # Negative batch size
            metrics.record_redis_operation("invalid_op", "unknown_status")
        except Exception as e:
            pytest.fail(f"MetricsCollector should handle invalid inputs gracefully: {e}")

        # Should still produce valid metrics
        metrics_data, _ = get_metrics_response()
        assert len(metrics_data) > 0

    @pytest.mark.integration
    def test_metrics_endpoint_accessibility(self):
        """Test the metrics endpoint if it's running."""
        try:
            response = requests.get("http://localhost:8000/metrics", timeout=5)
            assert response.status_code == 200
            assert "trip_events_total" in response.text
            assert len(response.text) > 0
        except requests.exceptions.RequestException:
            pytest.skip("Metrics endpoint not available (this is expected if not running)")

    def test_metrics_collector_registry_isolation(self):
        """Test that metrics are properly isolated in the registry."""
        # Create a collector and record some metrics
        metrics = MetricsCollector("isolation-test")
        metrics.record_trip_event("test_event")

        # Get metrics
        metrics_data, _ = get_metrics_response()
        metrics_text = metrics_data.decode("utf-8")

        # Verify the metric is present
        assert (
            'trip_events_total{event_type="test_event",service="isolation-test"} 1.0'
            in metrics_text
        )

        # Create another collector with same service name
        metrics2 = MetricsCollector("isolation-test")
        metrics2.record_trip_event("test_event")

        # Get metrics again
        metrics_data2, _ = get_metrics_response()
        metrics_text2 = metrics_data2.decode("utf-8")

        # Should now show 2.0 for the same metric
        assert (
            'trip_events_total{event_type="test_event",service="isolation-test"} 2.0'
            in metrics_text2
        )
