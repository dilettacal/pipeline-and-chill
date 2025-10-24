from pydantic import BaseModel
from datetime import datetime

class TripIdentity(BaseModel):
    trip_key: str
    vehicle_id_h: str
    pickup_ts: datetime
    source: str = "replay"
