"""
Event Ingestor Service - Expert Architecture

Consumes events from Kafka and dumps them to Redis.
Simple job: Kafka → Redis (with basic normalization).
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional

from core.clients.redis import RedisClient
from core.utils.logging import get_logger
from kafka import KafkaConsumer

logger = get_logger("event-ingestor")


class EventIngestor:
    """
    Event Ingestor - moves events from Kafka to Redis.

    Simple job:
    1. Consume events from Kafka
    2. Normalize and store in Redis
    3. Emit to trips.state-upsert topic (future)
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        topic: str = "trip-events",
        group_id: str = "event-ingestor",
        redis_url: str = "redis://localhost:6379/0",
        init_kafka: bool = True,
    ):
        """
        Initialize event ingestor.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            redis_url: Redis connection URL
        """
        self.topic = topic
        self.group_id = group_id

        # Initialize Kafka consumer (only if requested)
        self.kafka_consumer = None
        if init_kafka:
            self.kafka_consumer = KafkaConsumer(
                topic,
                bootstrap_servers=kafka_bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # Manual commit
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                max_poll_records=500,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )

        # Initialize Redis client
        self.redis_client = RedisClient(redis_url=redis_url)

        logger.info(
            "Event Ingestor initialized", topic=topic, group_id=group_id, redis_url=redis_url
        )

    def normalize_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize event data for consistent storage.

        Args:
            event: Raw event from Kafka

        Returns:
            Normalized event data
        """
        # Extract basic fields
        trip_key = event.get("trip_key")
        event_type = event.get("event_type")
        timestamp = event.get("timestamp")

        if not trip_key or not event_type:
            logger.warning("Invalid event - missing trip_key or event_type", event_data=event)
            return None

        # Normalize timestamp
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                logger.warning("Invalid timestamp format", timestamp=timestamp)
                timestamp = datetime.now()

        return {
            "trip_key": trip_key,
            "event_type": event_type,
            "timestamp": timestamp.isoformat(),
            "event_id": event.get("event_id"),
            "vendor_id": event.get("vendor_id"),
            "vehicle_id_h": event.get("vehicle_id_h"),
            "pickup_zone_id": event.get("pickup_zone_id"),
            "dropoff_zone_id": event.get("dropoff_zone_id"),
            "passenger_count": event.get("passenger_count"),
            "fare_amount": event.get("fare_amount"),
            "tip_amount": event.get("tip_amount"),
            "total_amount": event.get("total_amount"),
            "payment_type": event.get("payment_type"),
            "distance_km": event.get("distance_km"),
            "duration_min": event.get("duration_min"),
            "avg_speed_kmh": event.get("avg_speed_kmh"),
        }

    def store_event_in_redis(self, normalized_event: Dict[str, Any]) -> bool:
        """
        Store normalized event in Redis.

        Args:
            normalized_event: Normalized event data

        Returns:
            True if stored successfully, False otherwise
        """
        try:
            trip_key = normalized_event["trip_key"]
            event_type = normalized_event["event_type"]

            # Create Redis key
            redis_key = f"trip:{trip_key}"

            # Store event data in Redis hash
            # For now, just store the latest event data
            # TODO: Implement proper bitmask and atomic updates
            event_data = {
                f"{event_type}_timestamp": normalized_event["timestamp"],
            }

            # Only add event_id if it exists
            if normalized_event.get("event_id") is not None:
                event_data[f"{event_type}_event_id"] = str(normalized_event["event_id"])

            # Add common fields
            for field in [
                "vendor_id",
                "vehicle_id_h",
                "pickup_zone_id",
                "dropoff_zone_id",
                "passenger_count",
                "fare_amount",
                "tip_amount",
                "total_amount",
                "payment_type",
                "distance_km",
                "duration_min",
                "avg_speed_kmh",
            ]:
                if normalized_event.get(field) is not None:
                    event_data[field] = str(normalized_event[field])

            # Store in Redis with TTL (6 hours = 21600 seconds)
            self.redis_client.client.hset(redis_key, mapping=event_data)
            self.redis_client.client.expire(redis_key, 21600)

            logger.info(
                "Event stored in Redis",
                trip_key=trip_key,
                event_type=event_type,
                redis_key=redis_key,
            )

            return True

        except Exception as e:
            logger.error(
                "Failed to store event in Redis", error=str(e), event_data=normalized_event
            )
            return False

    def consume_events(self, max_events: Optional[int] = None) -> int:
        """
        Consume events from Kafka and store in Redis.

        Args:
            max_events: Maximum number of events to process

        Returns:
            Number of events processed
        """
        events_processed = 0

        logger.info("Starting event consumption", max_events=max_events)

        try:
            for message in self.kafka_consumer:
                try:
                    event = message.value
                    logger.info(
                        "Received event",
                        event_type=event.get("event_type"),
                        trip_key=event.get("trip_key"),
                    )

                    # Normalize event
                    normalized_event = self.normalize_event(event)
                    if not normalized_event:
                        continue

                    # Store in Redis
                    if self.store_event_in_redis(normalized_event):
                        # Commit offset after successful processing
                        self.kafka_consumer.commit()
                        events_processed += 1

                        logger.info(
                            "Event processed successfully", events_processed=events_processed
                        )
                    else:
                        logger.error("Failed to store event, not committing offset")

                    # Check limits
                    if max_events and events_processed >= max_events:
                        logger.info("Reached max events limit", max_events=max_events)
                        break

                except Exception as e:
                    logger.error("Error processing event", error=str(e))
                    # Don't commit offset on error - will retry

        except Exception as e:
            logger.error("Event consumption failed", error=str(e))
            raise

        logger.info("Event consumption complete", events_processed=events_processed)
        return events_processed

    def close(self):
        """Close connections and cleanup resources."""
        logger.info("Closing event ingestor...")
        self.kafka_consumer.close()
        logger.info("Event ingestor closed")
