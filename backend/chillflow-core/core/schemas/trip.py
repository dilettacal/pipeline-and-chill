"""
Pydantic schemas for trip-related data validation and serialization.

This module contains Pydantic models for validating and serializing
trip data in the ChillFlow system.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ZoneSchema(BaseModel):
    """Pydantic schema for taxi zone data."""

    zone_id: int = Field(..., description="Zone ID (1-265)")
    borough: str = Field(..., max_length=50, description="Borough name")
    zone_name: str = Field(..., max_length=100, description="Zone name")
    service_zone: str = Field(..., max_length=50, description="Service zone")

    model_config = ConfigDict(
        from_attributes=True, json_encoders={datetime: lambda v: v.isoformat()}
    )


class CompleteTripSchema(BaseModel):
    """Pydantic schema for complete trip data."""

    # Primary key
    trip_key: str = Field(..., max_length=64, description="Unique trip identifier")

    # Trip metadata
    vendor_id: int = Field(..., ge=1, le=6, description="Taxi vendor ID")
    pickup_ts: datetime = Field(..., description="Pickup timestamp")
    dropoff_ts: datetime = Field(..., description="Dropoff timestamp")

    # Location data
    pu_zone_id: int = Field(..., ge=1, le=265, description="Pickup zone ID")
    do_zone_id: int = Field(..., ge=1, le=265, description="Dropoff zone ID")

    # Trip details
    passenger_count: Optional[int] = Field(None, ge=0, le=9, description="Number of passengers")
    trip_distance: Optional[float] = Field(None, ge=0, description="Trip distance in miles")
    fare_amount: Optional[float] = Field(None, ge=0, description="Fare amount in USD")
    tip_amount: Optional[float] = Field(None, ge=0, description="Tip amount in USD")
    total_amount: Optional[float] = Field(None, ge=0, description="Total amount in USD")
    payment_type: Optional[int] = Field(None, ge=1, le=6, description="Payment type code")

    # Vehicle information
    vehicle_id_h: str = Field(..., max_length=16, description="Hashed vehicle ID")

    # Audit fields
    created_at: Optional[datetime] = Field(None, description="Record creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Record update timestamp")

    @field_validator("dropoff_ts")
    @classmethod
    def validate_dropoff_after_pickup(cls, v, info):
        """Validate that dropoff is after pickup."""
        if info.data.get("pickup_ts") and v <= info.data["pickup_ts"]:
            raise ValueError("Dropoff time must be after pickup time")
        return v

    @field_validator("total_amount")
    @classmethod
    def validate_total_amount(cls, v, info):
        """Validate total amount consistency.

        Note: NYC taxi data includes additional fees (tolls, surcharges, etc.)
        that are not captured in fare_amount + tip_amount, so we only validate
        that total_amount is reasonable (not negative and not extremely high).
        """
        if v is not None:
            # Basic sanity checks
            if v < 0:
                raise ValueError("Total amount cannot be negative")
            if v > 1000:  # Reasonable upper bound for taxi fare
                raise ValueError(f"Total amount {v} seems unreasonably high")
        return v

    @property
    def trip_duration_minutes(self) -> Optional[float]:
        """Calculate trip duration in minutes."""
        if self.pickup_ts and self.dropoff_ts:
            delta = self.dropoff_ts - self.pickup_ts
            return delta.total_seconds() / 60
        return None

    @property
    def has_tip(self) -> bool:
        """Check if trip has a tip."""
        return self.tip_amount is not None and self.tip_amount > 0

    model_config = ConfigDict(
        from_attributes=True, json_encoders={datetime: lambda v: v.isoformat()}
    )


class TripCreateSchema(BaseModel):
    """Schema for creating new trip records."""

    vendor_id: int = Field(..., ge=1, le=6)
    pickup_ts: datetime = Field(...)
    dropoff_ts: datetime = Field(...)
    pu_zone_id: int = Field(..., ge=1, le=265)
    do_zone_id: int = Field(..., ge=1, le=265)
    passenger_count: Optional[int] = Field(None, ge=0, le=9)
    trip_distance: Optional[float] = Field(None, ge=0)
    fare_amount: Optional[float] = Field(None, ge=0)
    tip_amount: Optional[float] = Field(None, ge=0)
    total_amount: Optional[float] = Field(None, ge=0)
    payment_type: Optional[int] = Field(None, ge=1, le=6)
    vehicle_id_h: str = Field(..., max_length=16)

    @field_validator("dropoff_ts")
    @classmethod
    def validate_dropoff_after_pickup(cls, v, info):
        """Validate that dropoff is after pickup."""
        if info.data.get("pickup_ts") and v <= info.data["pickup_ts"]:
            raise ValueError("Dropoff time must be after pickup time")
        return v


class TripUpdateSchema(BaseModel):
    """Schema for updating existing trip records."""

    passenger_count: Optional[int] = Field(None, ge=0, le=9)
    trip_distance: Optional[float] = Field(None, ge=0)
    fare_amount: Optional[float] = Field(None, ge=0)
    tip_amount: Optional[float] = Field(None, ge=0)
    total_amount: Optional[float] = Field(None, ge=0)
    payment_type: Optional[int] = Field(None, ge=1, le=6)

    model_config = ConfigDict(from_attributes=True)
