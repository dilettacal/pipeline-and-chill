"""
Trip Builder - Builds CompleteTrip objects from normalized snapshots.

This module provides a pure function to convert trip snapshots into CompleteTrip objects,
handling missing fields gracefully with sensible defaults.
"""

from typing import Any, Dict

from core import CompleteTrip
from core.utils.logging import get_logger

logger = get_logger("chillflow-stream.trip-builder")


class TripBuilder:
    """
    Builds CompleteTrip objects from normalized trip snapshots.

    This class handles the conversion from Redis hash data to CompleteTrip objects,
    providing sensible defaults for missing fields.
    """

    def build(self, trip_key: str, snapshot: Dict[str, Any]) -> CompleteTrip:
        """
        Build a CompleteTrip from a normalized snapshot.

        Args:
            trip_key: The trip identifier
            snapshot: Normalized trip data from Redis

        Returns:
            CompleteTrip object with all available data
        """
        try:
            # Extract and convert fields with defaults
            vendor_id = self._safe_int(snapshot.get("vendor_id"), 0)
            fare_amount = self._safe_float(snapshot.get("fare_amount"), 0.0)
            passenger_count = self._safe_int(snapshot.get("passenger_count"), 0)
            tip_amount = self._safe_float(snapshot.get("tip_amount"), 0.0)
            total_amount = self._safe_float(snapshot.get("total_amount"), 0.0)

            # Create CompleteTrip object
            complete_trip = CompleteTrip(
                trip_key=trip_key,
                vendor_id=vendor_id,
                fare_amount=fare_amount,
                passenger_count=passenger_count,
                tip_amount=tip_amount,
                total_amount=total_amount,
                # Add other fields as needed
            )

            logger.info("Trip built successfully", trip_key=trip_key)
            return complete_trip

        except Exception as e:
            logger.error("Failed to build trip", trip_key=trip_key, error=str(e))
            raise

    def _safe_int(self, value: Any, default: int) -> int:
        """Safely convert value to int with default."""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _safe_float(self, value: Any, default: float) -> float:
        """Safely convert value to float with default."""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
