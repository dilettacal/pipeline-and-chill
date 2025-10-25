"""
Event schemas for streaming trip data.

This module defines the event types that are generated when trips are split
into individual signals/events for real-time processing.
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Types of trip events that can be generated."""

    TRIP_STARTED = "trip_started"
    TRIP_ENDED = "trip_ended"
    PASSENGER_PICKED_UP = "passenger_picked_up"
    PASSENGER_DROPPED_OFF = "passenger_dropped_off"
    PAYMENT_PROCESSED = "payment_processed"
    ZONE_ENTERED = "zone_entered"
    ZONE_EXITED = "zone_exited"


class TripEvent(BaseModel):
    """Base event schema for all trip-related events."""

    event_id: str = Field(..., description="Unique event identifier")
    trip_key: str = Field(..., description="Trip identifier this event belongs to")
    event_type: EventType = Field(..., description="Type of event")
    timestamp: datetime = Field(..., description="When the event occurred")
    vendor_id: int = Field(..., description="Taxi vendor ID")
    vehicle_id_h: str = Field(..., description="Hashed vehicle identifier")
    source: str = Field(default="stream", description="Event source")

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }


class TripStartedEvent(TripEvent):
    """Event when a trip begins."""

    event_type: EventType = Field(default=EventType.TRIP_STARTED, description="Event type")
    pickup_zone_id: int = Field(..., description="Pickup zone ID")
    passenger_count: Optional[int] = Field(None, description="Number of passengers")


class TripEndedEvent(TripEvent):
    """Event when a trip ends."""

    event_type: EventType = Field(default=EventType.TRIP_ENDED, description="Event type")
    dropoff_zone_id: int = Field(..., description="Dropoff zone ID")
    trip_distance: Optional[float] = Field(None, description="Trip distance in km")
    trip_duration_minutes: Optional[float] = Field(None, description="Trip duration in minutes")


class PaymentProcessedEvent(TripEvent):
    """Event when payment is processed."""

    event_type: EventType = Field(default=EventType.PAYMENT_PROCESSED, description="Event type")
    fare_amount: Optional[float] = Field(None, description="Fare amount")
    tip_amount: Optional[float] = Field(None, description="Tip amount")
    total_amount: Optional[float] = Field(None, description="Total amount")
    payment_type: Optional[int] = Field(None, description="Payment method type")


class ZoneEvent(TripEvent):
    """Event when entering or exiting a zone."""

    zone_id: int = Field(..., description="Zone ID")
    zone_name: Optional[str] = Field(None, description="Zone name")
    borough: Optional[str] = Field(None, description="Borough name")


class ZoneEnteredEvent(ZoneEvent):
    """Event when entering a zone."""

    event_type: EventType = Field(default=EventType.ZONE_ENTERED, description="Event type")


class ZoneExitedEvent(ZoneEvent):
    """Event when exiting a zone."""

    event_type: EventType = Field(default=EventType.ZONE_EXITED, description="Event type")
