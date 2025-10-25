from datetime import datetime

from pydantic import BaseModel


class TripIdentity(BaseModel):
    trip_key: str
    vehicle_id_h: str
    pickup_ts: datetime
    source: str = "replay"
