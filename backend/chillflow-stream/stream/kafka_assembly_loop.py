"""
Kafka Assembly Loop - Orchestrates Kafka consumption and trip assembly.

This module provides the main orchestration loop that:
- Consumes events from Kafka
- Processes them through the TripAssemblerProcessor
- Writes complete trips to a sink (DB writer or Kafka producer)
- Handles batch commits for performance
"""

from typing import Dict, Optional

from core.utils.logging import get_logger

from .trip_assembler_processor import TripAssemblerProcessor


class KafkaAssemblyLoop:
    """
    Orchestrates the main processing loop for trip assembly.

    This class handles:
    - Kafka event consumption
    - Batch processing and offset commits
    - Complete trip forwarding to sinks
    """

    def __init__(self, consumer, processor: TripAssemblerProcessor, sink):
        """
        Initialize the assembly loop.

        Args:
            consumer: Kafka consumer (adapter over kafka-python/confluent-kafka)
            processor: TripAssemblerProcessor for business logic
            sink: Sink for complete trips (DB writer or Kafka producer)
        """
        self.consumer = consumer
        self.processor = processor
        self.sink = sink
        self.log = get_logger("chillflow-stream.kafka-assembly-loop")

        self.log.info("Kafka Assembly Loop initialized")

    def run(self, max_records: Optional[int] = None, commit_every: int = 500) -> Dict[str, int]:
        """
        Run the main processing loop.

        Args:
            max_records: Maximum number of records to process (None for unlimited)
            commit_every: Commit offsets every N records

        Returns:
            Dictionary with processing statistics
        """
        completed = 0
        processed = 0
        batch = 0

        self.log.info("Starting assembly loop", max_records=max_records, commit_every=commit_every)

        try:
            for msg in self.consumer:
                # Process the event
                trip = self.processor.process(msg.value)

                if trip:
                    # Write complete trip to sink
                    try:
                        self.sink.write(trip)
                        completed += 1
                        self.log.info("Complete trip forwarded", trip_key=trip.trip_key)
                    except Exception as e:
                        self.log.error(
                            "Failed to write complete trip", trip_key=trip.trip_key, error=str(e)
                        )

                processed += 1
                batch += 1

                # Commit offsets periodically
                if batch >= commit_every:
                    try:
                        self.consumer.commit()
                        self.log.debug("Offsets committed", batch_size=batch)
                        batch = 0
                    except Exception as e:
                        self.log.error("Failed to commit offsets", error=str(e))

                # Check if we've reached the limit
                if max_records and processed >= max_records:
                    self.log.info("Reached max records limit", processed=processed)
                    break

        except Exception as e:
            self.log.error("Error in assembly loop", error=str(e))
            raise

        # Commit any remaining offsets
        if batch > 0:
            try:
                self.consumer.commit()
                self.log.debug("Final offsets committed", batch_size=batch)
            except Exception as e:
                self.log.error("Failed to commit final offsets", error=str(e))

        result = {"events": processed, "trips_completed": completed}

        self.log.info("Assembly loop completed", **result)
        return result
