"""
Test Event Ingestor - Expert Architecture Step 1
"""

from unittest.mock import Mock, patch

from stream.event_ingestor import EventIngestor


class TestEventIngestor:
    """Test Event Ingestor basic functionality."""

    def test_initialization(self):
        """Test Event Ingestor initializes correctly."""
        with (
            patch("stream.event_ingestor.KafkaConsumer"),
            patch("stream.event_ingestor.RedisClient"),
        ):

            ingestor = EventIngestor(
                kafka_bootstrap_servers="localhost:9092",
                topic="test-topic",
                group_id="test-group",
                redis_url="redis://localhost:6379/0",
            )

            assert ingestor.topic == "test-topic"
            assert ingestor.group_id == "test-group"

    def test_normalize_event_valid(self):
        """Test event normalization with valid data."""
        with (
            patch("stream.event_ingestor.KafkaConsumer"),
            patch("stream.event_ingestor.RedisClient"),
        ):

            ingestor = EventIngestor()

            # Test valid event
            event = {
                "trip_key": "test-trip-123",
                "event_type": "trip_started",
                "timestamp": "2024-01-01T12:00:00",
                "vendor_id": 1,
                "pickup_zone_id": 100,
            }

            normalized = ingestor.normalize_event(event)

            assert normalized is not None
            assert normalized["trip_key"] == "test-trip-123"
            assert normalized["event_type"] == "trip_started"
            assert normalized["vendor_id"] == 1
            assert normalized["pickup_zone_id"] == 100

    def test_normalize_event_invalid(self):
        """Test event normalization with invalid data."""
        with (
            patch("stream.event_ingestor.KafkaConsumer"),
            patch("stream.event_ingestor.RedisClient"),
        ):

            ingestor = EventIngestor()

            # Test invalid event (missing trip_key)
            event = {"event_type": "trip_started", "timestamp": "2024-01-01T12:00:00"}

            normalized = ingestor.normalize_event(event)

            assert normalized is None

    def test_store_event_in_redis_success(self):
        """Test storing event in Redis successfully."""
        with (
            patch("stream.event_ingestor.KafkaConsumer"),
            patch("stream.event_ingestor.RedisClient") as mock_redis,
        ):

            # Mock Redis client
            mock_redis_client = Mock()
            mock_redis.return_value = mock_redis_client
            mock_redis_client.client.hset.return_value = True
            mock_redis_client.client.expire.return_value = True

            ingestor = EventIngestor()

            # Test storing event
            event = {
                "trip_key": "test-trip-123",
                "event_type": "trip_started",
                "timestamp": "2024-01-01T12:00:00",
                "vendor_id": 1,
            }

            result = ingestor.store_event_in_redis(event)

            assert result is True
            mock_redis_client.client.hset.assert_called_once()
            mock_redis_client.client.expire.assert_called_once()

    def test_store_event_in_redis_failure(self):
        """Test storing event in Redis with failure."""
        with (
            patch("stream.event_ingestor.KafkaConsumer"),
            patch("stream.event_ingestor.RedisClient") as mock_redis,
        ):

            # Mock Redis client to raise exception
            mock_redis_client = Mock()
            mock_redis.return_value = mock_redis_client
            mock_redis_client.client.hset.side_effect = Exception("Redis error")

            ingestor = EventIngestor()

            # Test storing event
            event = {
                "trip_key": "test-trip-123",
                "event_type": "trip_started",
                "timestamp": "2024-01-01T12:00:00",
            }

            result = ingestor.store_event_in_redis(event)

            assert result is False
