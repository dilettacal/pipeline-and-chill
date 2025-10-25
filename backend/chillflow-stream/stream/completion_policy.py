"""
Completion Policy - Defines what makes a trip complete.

This module centralizes the logic for determining when a trip is complete,
using both set-based and bitmask-based approaches for efficiency.
"""

from typing import Set

from .events import EventType


class CompletionPolicy:
    """
    Defines the policy for trip completion.

    A trip is considered complete when all required events are present.
    This class provides both set-based and bitmask-based completion checks.
    """

    # Required events for a complete trip
    REQUIRED: Set[EventType] = {
        EventType.TRIP_STARTED,
        EventType.TRIP_ENDED,
        EventType.PAYMENT_PROCESSED,
    }

    @classmethod
    def is_complete_set(cls, present_events: Set[EventType]) -> bool:
        """
        Check if a trip is complete based on present events.

        Args:
            present_events: Set of event types that are present

        Returns:
            True if trip is complete, False otherwise
        """
        return cls.REQUIRED.issubset(present_events)

    @classmethod
    def is_complete_mask(cls, mask: int) -> bool:
        """
        Check if a trip is complete based on bitmask.

        Args:
            mask: Bitmask representing present events

        Returns:
            True if trip is complete, False otherwise
        """
        required_mask = 0
        for event_type in cls.REQUIRED:
            required_mask |= 1 << event_type.mask_bit()

        return (mask & required_mask) == required_mask

    @classmethod
    def missing_from(cls, present_events: Set[EventType]) -> Set[EventType]:
        """
        Get the missing events for a trip.

        Args:
            present_events: Set of event types that are present

        Returns:
            Set of missing event types
        """
        return cls.REQUIRED - present_events
