"""
Kafka helper functions for integration tests.
"""

from kafka.admin import KafkaAdminClient, NewTopic


def ensure_topic(bootstrap: str, topic: str, partitions: int = 1, rf: int = 1):
    """Ensure a Kafka topic exists before tests run."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="test-admin")
    if topic not in set(admin.list_topics()):
        try:
            admin.create_topics(
                [NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)]
            )
        except Exception:
            # Ignore if created concurrently
            pass
    admin.close()
