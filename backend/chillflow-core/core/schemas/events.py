"""
Pydantic schemas for Kafka event messages.

This module contains Pydantic models for validating and serializing
event messages in the ChillFlow streaming system.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EventType(str, Enum):
    """Enumeration of event types in the ChillFlow system."""

    IDENTITY = "IDENTITY"
    PASSENGERS = "PASSENGERS"
    FARE = "FARE"
    DROP = "DROP"
    TIP = "TIP"


class BaseEvent(BaseModel):
    """Base event schema with common fields."""

    event_type: EventType = Field(..., description="Type of event")
    event_version: str = Field("1", description="Event schema version")
    trip_key: str = Field(..., max_length=64, description="Unique trip identifier")
    vehicle_id_h: str = Field(..., max_length=16, description="Hashed vehicle ID")
    event_ts: datetime = Field(..., description="Event timestamp")

    model_config = ConfigDict(
        use_enum_values=True, json_encoders={datetime: lambda v: v.isoformat()}
    )


class IdentityEvent(BaseEvent):
    """Identity event - trip start with basic metadata."""

    event_type: EventType = Field(EventType.IDENTITY, description="Event type")
    vendor_id: int = Field(..., ge=1, le=6, description="Taxi vendor ID")
    pickup_ts: datetime = Field(..., description="Pickup timestamp")
    pu_zone_id: int = Field(..., ge=1, le=265, description="Pickup zone ID")
    do_zone_id: int = Field(..., ge=1, le=265, description="Dropoff zone ID")

    @field_validator("event_ts")
    @classmethod
    def validate_event_ts_matches_pickup(cls, v, info):
        """Validate that event timestamp matches pickup time."""
        if info.data.get("pickup_ts") and v != info.data["pickup_ts"]:
            raise ValueError("Event timestamp must match pickup timestamp")
        return v


class PassengersEvent(BaseEvent):
    """Passengers event - passenger count information."""

    event_type: EventType = Field(EventType.PASSENGERS, description="Event type")
    passenger_count: int = Field(..., ge=0, le=9, description="Number of passengers")

    @field_validator("event_ts")
    @classmethod
    def validate_event_ts_after_identity(cls, v, info):
        """Validate that event timestamp is after identity event."""
        # This would need trip_key lookup in real implementation
        return v


class FareEvent(BaseEvent):
    """Fare event - fare amount and distance information."""

    event_type: EventType = Field(EventType.FARE, description="Event type")
    fare_amount: float = Field(..., ge=0, description="Fare amount in USD")
    trip_distance: float = Field(..., ge=0, description="Trip distance in miles")
    payment_type: int = Field(..., ge=1, le=6, description="Payment type code")

    @field_validator("event_ts")
    @classmethod
    def validate_event_ts_between_pickup_dropoff(cls, v, info):
        """Validate that event timestamp is between pickup and dropoff."""
        # This would need trip_key lookup in real implementation
        return v


class DropEvent(BaseEvent):
    """Drop event - trip completion."""

    event_type: EventType = Field(EventType.DROP, description="Event type")
    dropoff_ts: datetime = Field(..., description="Dropoff timestamp")
    do_zone_id: int = Field(..., ge=1, le=265, description="Dropoff zone ID")
    total_amount: float = Field(..., ge=0, description="Total amount in USD")

    @field_validator("event_ts")
    @classmethod
    def validate_event_ts_matches_dropoff(cls, v, info):
        """Validate that event timestamp matches dropoff time."""
        if info.data.get("dropoff_ts") and v != info.data["dropoff_ts"]:
            raise ValueError("Event timestamp must match dropoff timestamp")
        return v


class TipEvent(BaseEvent):
    """Tip event - late-arriving tip information."""

    event_type: EventType = Field(EventType.TIP, description="Event type")
    tip_amount: float = Field(..., ge=0, description="Tip amount in USD")

    @field_validator("event_ts")
    @classmethod
    def validate_event_ts_after_dropoff(cls, v, info):
        """Validate that event timestamp is after dropoff."""
        # This would need trip_key lookup in real implementation
        return v


class TripEventUnion(BaseModel):
    """Union type for all trip events."""

    event: Dict[str, Any] = Field(..., description="Event data")

    @field_validator("event")
    @classmethod
    def validate_event_type(cls, v):
        """Validate that event has required fields."""
        required_fields = ["event_type", "trip_key", "vehicle_id_h", "event_ts"]
        for field in required_fields:
            if field not in v:
                raise ValueError(f"Missing required field: {field}")
        return v


# Event type mapping for deserialization
EVENT_TYPE_MAPPING = {
    EventType.IDENTITY: IdentityEvent,
    EventType.PASSENGERS: PassengersEvent,
    EventType.FARE: FareEvent,
    EventType.DROP: DropEvent,
    EventType.TIP: TipEvent,
}


def deserialize_event(event_data: Dict[str, Any]) -> BaseEvent:
    """
    Deserialize event data into appropriate event type.

    Args:
        event_data: Raw event data dictionary

    Returns:
        Parsed event object

    Raises:
        ValueError: If event type is unknown or data is invalid
    """
    event_type = event_data.get("event_type")
    if event_type not in EVENT_TYPE_MAPPING:
        raise ValueError(f"Unknown event type: {event_type}")

    event_class = EVENT_TYPE_MAPPING[event_type]
    return event_class(**event_data)
