"""
Trip Assembler - Legacy stub for backward compatibility.

This is a stub implementation to maintain backward compatibility
while transitioning to the new architecture.
"""

from typing import Dict, Optional

from core import get_logger as get_core_logger

logger = get_core_logger("chillflow-stream.trip-assembler")


class TripAssembler:
    """
    Legacy TripAssembler stub for backward compatibility.

    This class is deprecated and will be removed in future versions.
    Use the new architecture components instead:
    - TripAssemblerProcessor
    - KafkaAssemblyLoop
    - TripDbWriter
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        topic: str = "trip-events",
        group_id: str = "trip-assembler",
        use_redis: bool = False,
        redis_url: str = "redis://localhost:6379/0",
        init_kafka: bool = True,
    ):
        """
        Initialize the trip assembler stub.
        """
        self.topic = topic
        self.group_id = group_id
        self.partial_trips = {}
        self.completed_trips = []

        logger.warning(
            "TripAssembler is deprecated. Use new architecture components instead.",
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic=topic,
            group_id=group_id,
            use_redis=use_redis,
        )

    def run_assembly_loop(
        self,
        timeout_ms: int = 30000,
        max_events: Optional[int] = None,
        save_to_db: bool = True,
    ) -> Dict[str, int]:
        """
        Run assembly loop stub.

        Returns:
            Dictionary with assembly statistics
        """
        logger.warning("TripAssembler.run_assembly_loop is deprecated")

        return {
            "trips_assembled": 0,
            "trips_saved": 0,
            "trips_failed": 0,
        }

    def get_partial_trip_count(self) -> int:
        """Get count of partial trips."""
        return len(self.partial_trips)

    def get_completed_trip_count(self) -> int:
        """Get count of completed trips."""
        return len(self.completed_trips)

    def close(self):
        """Close the assembler."""
        logger.info("TripAssembler stub closed")
