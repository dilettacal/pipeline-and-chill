"""
Deterministic hashing functions for trip keys and vehicle IDs.

These functions generate stable, unique identifiers for trips and vehicles
using SHA-256 hashing. The same inputs always produce the same outputs,
enabling idempotent data processing.
"""

import hashlib
from datetime import datetime


def sha256_hex(s: str) -> str:
    """
    Compute SHA-256 hash of a string and return as hex.
    
    Args:
        s: Input string to hash
        
    Returns:
        64-character hexadecimal hash
    """
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def generate_trip_key(
    salt: str,
    vendor_id: int,
    pickup_ts: datetime,
    pu_zone_id: int,
    row_offset: int,
) -> str:
    """
    Generate a deterministic unique identifier for a trip.
    
    The trip key is used as the primary key in stg.complete_trip and ensures
    idempotent writes. The same trip data always produces the same key.
    
    Algorithm:
        trip_key = SHA256(salt|vendor_id|pickup_ts_iso|pu_zone_id|row_offset)
    
    Args:
        salt: Secret salt for hash uniqueness (from HASH_SALT env var)
        vendor_id: Taxi vendor ID (1, 2, or 6)
        pickup_ts: Trip pickup timestamp
        pu_zone_id: Pickup zone ID (1-265)
        row_offset: Row number in source file (0-based)
        
    Returns:
        64-character hexadecimal string (SHA-256 hash)
        
    Examples:
        >>> from datetime import datetime
        >>> generate_trip_key(
        ...     salt="test-salt",
        ...     vendor_id=1,
        ...     pickup_ts=datetime(2025, 1, 1, 0, 18, 38),
        ...     pu_zone_id=229,
        ...     row_offset=0
        ... )
        'e8f4d2a1b3c5e7f9...'  # 64-char hex
    """
    # Format timestamp as ISO 8601 for consistency
    pickup_ts_iso = pickup_ts.isoformat()
    
    # Build payload with pipe separator
    payload = f"{salt}|{vendor_id}|{pickup_ts_iso}|{pu_zone_id}|{row_offset}"
    
    # Compute SHA-256 hash
    return sha256_hex(payload)


def generate_vehicle_id_h(
    salt: str,
    vendor_id: int,
    row_offset: int,
    fleet_size: int = 5000,
) -> str:
    """
    Generate a synthetic vehicle identifier.
    
    Since NYC Taxi data doesn't include vehicle IDs, we generate synthetic
    ones using deterministic hashing. The modulo operation simulates a fleet
    of vehicles that appear multiple times in the dataset.
    
    Algorithm:
        vehicle_id_h = "veh_" + SHA256(salt|"vehicle"|vendor_id|row_offset % fleet_size)[:12]
    
    Args:
        salt: Secret salt for hash uniqueness
        vendor_id: Taxi vendor ID (1, 2, or 6)
        row_offset: Row number in source file (0-based)
        fleet_size: Simulated fleet size (default: 5000 vehicles)
        
    Returns:
        Vehicle ID string in format "veh_XXXXXXXXXXXX" (16 chars total)
        
    Examples:
        >>> generate_vehicle_id_h(
        ...     salt="test-salt",
        ...     vendor_id=1,
        ...     row_offset=0,
        ...     fleet_size=5000
        ... )
        'veh_a3f5b8c2d1e4'
        
    Note:
        The fleet_size modulo ensures the same vehicle IDs repeat across
        trips, simulating a realistic fleet where vehicles make multiple
        trips per day.
    """
    # Apply modulo to simulate fleet reuse
    vehicle_index = row_offset % fleet_size
    
    # Build payload
    payload = f"{salt}|vehicle|{vendor_id}|{vehicle_index}"
    
    # Compute hash and take first 12 characters
    hash_full = sha256_hex(payload)
    hash_short = hash_full[:12]
    
    # Format as vehicle ID
    return f"veh_{hash_short}"
