"""
Kafka client for ChillFlow with producer and consumer management.

This module provides Kafka clients for producing and consuming messages
in the ChillFlow streaming system.
"""

import json
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from core.settings import settings
from core.utils.logging import get_logger

logger = get_logger("kafka-client")


class KafkaProducerClient:
    """Kafka producer client with message serialization."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """
        Initialize Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers. Uses settings if not provided.
        """
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.producer: Optional[KafkaProducer] = None
        self._setup_producer()

    def _setup_producer(self) -> None:
        """Setup Kafka producer with configuration."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                batch_size=16384,
                linger_ms=5,
                buffer_memory=33554432,
            )

            logger.info("Kafka producer initialized", servers=self.bootstrap_servers)

        except Exception as e:
            logger.error("Failed to initialize Kafka producer", error=str(e))
            raise

    def send_message(self, topic: str, message: Any, key: Optional[str] = None) -> bool:
        """
        Send message to Kafka topic.

        Args:
            topic: Kafka topic name
            message: Message to send (will be JSON serialized)
            key: Optional message key

        Returns:
            True if message was sent successfully
        """
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")

        try:
            future = self.producer.send(topic, value=message, key=key)
            record_metadata = future.get(timeout=10)

            logger.debug(
                "Message sent to Kafka",
                topic=topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
            )
            return True

        except KafkaError as e:
            logger.error("Kafka send error", topic=topic, error=str(e))
            return False
        except Exception as e:
            logger.error("Unexpected error sending to Kafka", topic=topic, error=str(e))
            return False

    def send_batch(self, topic: str, messages: List[Dict[str, Any]]) -> int:
        """
        Send batch of messages to Kafka topic.

        Args:
            topic: Kafka topic name
            messages: List of messages to send

        Returns:
            Number of messages sent successfully
        """
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")

        sent_count = 0
        for message in messages:
            if self.send_message(topic, message):
                sent_count += 1

        logger.info(
            "Batch sent to Kafka", topic=topic, sent=sent_count, total=len(messages)
        )
        return sent_count

    def flush(self) -> None:
        """Flush producer buffers."""
        if self.producer:
            self.producer.flush()

    def close(self) -> None:
        """Close Kafka producer."""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


class KafkaConsumerClient:
    """Kafka consumer client with message deserialization."""

    def __init__(
        self,
        topics: List[str],
        group_id: str,
        bootstrap_servers: Optional[str] = None,
        auto_offset_reset: str = "earliest",
    ):
        """
        Initialize Kafka consumer.

        Args:
            topics: List of topics to consume
            group_id: Consumer group ID
            bootstrap_servers: Kafka bootstrap servers. Uses settings if not provided.
            auto_offset_reset: Offset reset strategy
        """
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.auto_offset_reset = auto_offset_reset
        self.consumer: Optional[KafkaConsumer] = None
        self._setup_consumer()

    def _setup_consumer(self) -> None:
        """Setup Kafka consumer with configuration."""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                max_poll_records=100,
                max_poll_interval_ms=300000,
            )

            logger.info(
                "Kafka consumer initialized",
                topics=self.topics,
                group_id=self.group_id,
                servers=self.bootstrap_servers,
            )

        except Exception as e:
            logger.error("Failed to initialize Kafka consumer", error=str(e))
            raise

    def consume_messages(self, timeout_ms: int = 1000) -> List[Dict[str, Any]]:
        """
        Consume messages from Kafka topics.

        Args:
            timeout_ms: Poll timeout in milliseconds

        Returns:
            List of consumed messages
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer not initialized")

        messages = []
        try:
            message_pack = self.consumer.poll(timeout_ms=timeout_ms)

            for topic_partition, records in message_pack.items():
                for record in records:
                    message = {
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "key": record.key,
                        "value": record.value,
                        "timestamp": record.timestamp,
                    }
                    messages.append(message)

            if messages:
                logger.debug(
                    "Consumed messages from Kafka",
                    count=len(messages),
                    topics=self.topics,
                )

        except Exception as e:
            logger.error("Error consuming messages from Kafka", error=str(e))
            raise

        return messages

    def commit_offsets(self) -> None:
        """Commit consumer offsets."""
        if self.consumer:
            self.consumer.commit()

    def close(self) -> None:
        """Close Kafka consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


class KafkaClient:
    """Unified Kafka client with producer and consumer management."""

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """
        Initialize unified Kafka client.

        Args:
            bootstrap_servers: Kafka bootstrap servers. Uses settings if not provided.
        """
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.producer_client: Optional[KafkaProducerClient] = None
        self.consumer_clients: Dict[str, KafkaConsumerClient] = {}

    def get_producer(self) -> KafkaProducerClient:
        """
        Get or create Kafka producer.

        Returns:
            Kafka producer client
        """
        if not self.producer_client:
            self.producer_client = KafkaProducerClient(self.bootstrap_servers)
        return self.producer_client

    def get_consumer(
        self, topics: List[str], group_id: str, auto_offset_reset: str = "earliest"
    ) -> KafkaConsumerClient:
        """
        Get or create Kafka consumer.

        Args:
            topics: List of topics to consume
            group_id: Consumer group ID
            auto_offset_reset: Offset reset strategy

        Returns:
            Kafka consumer client
        """
        consumer_key = f"{group_id}:{','.join(sorted(topics))}"

        if consumer_key not in self.consumer_clients:
            self.consumer_clients[consumer_key] = KafkaConsumerClient(
                topics=topics,
                group_id=group_id,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset=auto_offset_reset,
            )

        return self.consumer_clients[consumer_key]

    def health_check(self) -> bool:
        """
        Check Kafka connection health.

        Returns:
            True if Kafka is accessible, False otherwise
        """
        try:
            # Try to create a temporary producer to test connection
            temp_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            temp_producer.close()
            logger.debug("Kafka health check passed")
            return True
        except Exception as e:
            logger.error("Kafka health check failed", error=str(e))
            return False

    def close(self) -> None:
        """Close all Kafka connections."""
        if self.producer_client:
            self.producer_client.close()

        for consumer in self.consumer_clients.values():
            consumer.close()

        logger.info("All Kafka connections closed")


# Global Kafka client instance (lazy-loaded)
kafka_client: Optional[KafkaClient] = None


def get_kafka_client() -> KafkaClient:
    """
    Get the global Kafka client instance.

    Returns:
        Kafka client instance
    """
    global kafka_client
    if kafka_client is None:
        kafka_client = KafkaClient()
    return kafka_client
