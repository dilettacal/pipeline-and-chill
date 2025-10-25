"""
Trip DB Writer - Handles database persistence for complete trips.

This module provides database writing functionality for complete trips,
including single writes and batch operations for performance.
"""

from typing import List

from core import CompleteTrip
from core.clients.database import DatabaseClient
from core.utils.logging import get_logger


class TripDbWriter:
    """
    Handles database persistence for complete trips.

    This class provides:
    - Single trip writes
    - Batch trip writes for performance
    - Database connection management
    """

    def __init__(self, db_connection):
        """
        Initialize the DB writer.

        Args:
            db_connection: Database connection or URL
        """
        self.db_connection = db_connection
        self.log = get_logger("chillflow-stream.trip-db-writer")

        # Initialize database client
        if isinstance(db_connection, str):
            self.db_client = DatabaseClient(db_connection)
        else:
            # If we receive an Engine directly, create a DatabaseClient with it
            from sqlalchemy.orm import sessionmaker

            self.db_client = DatabaseClient()
            self.db_client.engine = db_connection
            self.db_client.SessionLocal = sessionmaker(bind=db_connection)

        self.log.info("Trip DB Writer initialized")

    def write(self, trip: CompleteTrip) -> bool:
        """
        Write a single complete trip to the database.

        Args:
            trip: CompleteTrip to write

        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info(
                "Writing trip to database",
                trip_key=trip.trip_key,
                vendor_id=trip.vendor_id,
                fare_amount=trip.fare_amount,
                passenger_count=trip.passenger_count,
                tip_amount=trip.tip_amount,
                total_amount=trip.total_amount,
            )

            # Use database client to write trip
            with self.db_client.get_session() as session:
                session.merge(trip)

            return True

        except Exception as e:
            self.log.error("Failed to write trip to database", trip_key=trip.trip_key, error=str(e))
            return False

    def write_batch(self, trips: List[CompleteTrip]) -> bool:
        """
        Write a batch of complete trips to the database.

        Args:
            trips: List of CompleteTrip objects to write

        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info("Writing batch of trips to database", count=len(trips))

            # Use database client to write batch
            with self.db_client.get_session() as session:
                for trip in trips:
                    session.merge(trip)

            return True

        except Exception as e:
            self.log.error("Failed to write batch to database", count=len(trips), error=str(e))
            return False
