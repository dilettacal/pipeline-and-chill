"""
ChillFlow Metrics - Prometheus metrics for monitoring
"""

import time

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
)

# Create a custom registry for our metrics
REGISTRY = CollectorRegistry()

# Application Info
APP_INFO = Info("chillflow_app_info", "Application information", registry=REGISTRY)

# Trip Processing Metrics
TRIP_EVENTS_TOTAL = Counter(
    "trip_events_total",
    "Total trip events processed",
    ["event_type", "service"],
    registry=REGISTRY,
)

TRIPS_COMPLETED_TOTAL = Counter(
    "trips_completed_total", "Total completed trips", ["service"], registry=REGISTRY
)

TRIPS_FAILED_TOTAL = Counter(
    "trips_failed_total",
    "Total failed trips",
    ["service", "error_type"],
    registry=REGISTRY,
)

# Processing Performance
PROCESSING_LATENCY_SEC = Histogram(
    "processing_latency_seconds",
    "End-to-end processing latency",
    ["service", "operation"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
    registry=REGISTRY,
)

BATCH_SIZE = Histogram(
    "batch_size",
    "Batch processing size",
    ["service"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
    registry=REGISTRY,
)

# Queue and State Metrics
QUEUE_LAG_SEC = Gauge(
    "queue_lag_seconds", "Kafka end-to-end lag estimate", ["service"], registry=REGISTRY
)

REDIS_OPERATIONS_TOTAL = Counter(
    "redis_operations_total",
    "Total Redis operations",
    ["operation", "status"],
    registry=REGISTRY,
)

REDIS_MEMORY_USAGE = Gauge("redis_memory_usage_bytes", "Redis memory usage", registry=REGISTRY)

# Data Quality Metrics
DQ_FAILURES_TOTAL = Counter(
    "dq_failures_total", "Data quality failures", ["rule", "service"], registry=REGISTRY
)

SCHEMA_VALIDATION_FAILURES = Counter(
    "schema_validation_failures_total",
    "Schema validation failures",
    ["field", "service"],
    registry=REGISTRY,
)

# Database Metrics
DB_OPERATIONS_TOTAL = Counter(
    "db_operations_total",
    "Database operations",
    ["operation", "table", "status"],
    registry=REGISTRY,
)

DB_CONNECTION_POOL = Gauge(
    "db_connection_pool_size",
    "Database connection pool size",
    ["state"],
    registry=REGISTRY,
)

# Business Metrics
REVENUE_TOTAL = Counter(
    "revenue_total", "Total revenue processed", ["zone", "service"], registry=REGISTRY
)

POPULAR_ZONES = Counter(
    "popular_zones_total", "Popular zone visits", ["zone", "service"], registry=REGISTRY
)

# System Health
HEALTH_CHECK = Gauge(
    "health_check",
    "Service health status (1=healthy, 0=unhealthy)",
    ["service"],
    registry=REGISTRY,
)

MEMORY_USAGE = Gauge("memory_usage_bytes", "Memory usage in bytes", ["service"], registry=REGISTRY)

CPU_USAGE = Gauge("cpu_usage_percent", "CPU usage percentage", ["service"], registry=REGISTRY)


class MetricsCollector:
    """Centralized metrics collector for ChillFlow services"""

    def __init__(self, service_name: str, version: str = "1.0.0"):
        self.service_name = service_name
        self.version = version

        # Set application info
        APP_INFO.info({"service": service_name, "version": version, "environment": "development"})

    def record_trip_event(self, event_type: str):
        """Record a trip event"""
        TRIP_EVENTS_TOTAL.labels(event_type=event_type, service=self.service_name).inc()

    def record_trip_completed(self):
        """Record a completed trip"""
        TRIPS_COMPLETED_TOTAL.labels(service=self.service_name).inc()

    def record_trip_failed(self, error_type: str):
        """Record a failed trip"""
        TRIPS_FAILED_TOTAL.labels(service=self.service_name, error_type=error_type).inc()

    def record_processing_time(self, operation: str, duration: float):
        """Record processing time"""
        PROCESSING_LATENCY_SEC.labels(service=self.service_name, operation=operation).observe(
            duration
        )

    def record_batch_size(self, size: int):
        """Record batch size"""
        BATCH_SIZE.labels(service=self.service_name).observe(size)

    def set_queue_lag(self, lag_seconds: float):
        """Set queue lag"""
        QUEUE_LAG_SEC.labels(service=self.service_name).set(lag_seconds)

    def record_redis_operation(self, operation: str, status: str):
        """Record Redis operation"""
        REDIS_OPERATIONS_TOTAL.labels(operation=operation, status=status).inc()

    def set_redis_memory_usage(self, bytes_used: int):
        """Set Redis memory usage"""
        REDIS_MEMORY_USAGE.set(bytes_used)

    def record_dq_failure(self, rule: str):
        """Record data quality failure"""
        DQ_FAILURES_TOTAL.labels(rule=rule, service=self.service_name).inc()

    def record_schema_validation_failure(self, field: str):
        """Record schema validation failure"""
        SCHEMA_VALIDATION_FAILURES.labels(field=field, service=self.service_name).inc()

    def record_db_operation(self, operation: str, table: str, status: str):
        """Record database operation"""
        DB_OPERATIONS_TOTAL.labels(operation=operation, table=table, status=status).inc()

    def set_db_connection_pool(self, active: int, idle: int):
        """Set database connection pool metrics"""
        DB_CONNECTION_POOL.labels(state="active").set(active)
        DB_CONNECTION_POOL.labels(state="idle").set(idle)

    def record_revenue(self, amount: float, zone: str):
        """Record revenue"""
        REVENUE_TOTAL.labels(zone=zone, service=self.service_name).inc(amount)

    def record_zone_visit(self, zone: str):
        """Record zone visit"""
        POPULAR_ZONES.labels(zone=zone, service=self.service_name).inc()

    def set_health_status(self, is_healthy: bool):
        """Set health status"""
        HEALTH_CHECK.labels(service=self.service_name).set(1 if is_healthy else 0)

    def set_memory_usage(self, bytes_used: int):
        """Set memory usage"""
        MEMORY_USAGE.labels(service=self.service_name).set(bytes_used)

    def set_cpu_usage(self, percent: float):
        """Set CPU usage"""
        CPU_USAGE.labels(service=self.service_name).set(percent)


def get_metrics_response():
    """Get metrics response for HTTP endpoint"""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


# Context manager for timing operations
class ProcessingTimer:
    """Context manager for timing operations"""

    def __init__(self, metrics: MetricsCollector, operation: str):
        self.metrics = metrics
        self.operation = operation
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.metrics.record_processing_time(self.operation, duration)
