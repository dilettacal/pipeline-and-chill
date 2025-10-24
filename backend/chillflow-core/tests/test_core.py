"""Test ChillFlow Core package functionality."""

import pytest
from core import generate_trip_key, generate_vehicle_id_h, settings, sha256_hex


def test_settings_loading():
    """Test that settings can be loaded correctly."""
    assert settings is not None
    assert hasattr(settings, "DATABASE_URL")
    assert hasattr(settings, "REDIS_URL")
    assert hasattr(settings, "KAFKA_BOOTSTRAP_SERVERS")
    assert hasattr(settings, "HASH_SALT")


def test_hashing_functions():
    """Test hashing utility functions."""
    from datetime import datetime

    # Test trip key generation
    trip_key = generate_trip_key(
        salt="test-salt",
        vendor_id=1,
        pickup_ts=datetime(2025, 1, 1, 0, 18, 38),
        pu_zone_id=229,
        row_offset=0,
    )
    assert isinstance(trip_key, str)
    assert len(trip_key) == 64  # SHA256 hex length

    # Test vehicle ID hashing
    vehicle_id = generate_vehicle_id_h(
        salt="test-salt", vendor_id=1, row_offset=0, fleet_size=5000
    )
    assert isinstance(vehicle_id, str)
    assert vehicle_id.startswith("veh_")
    assert len(vehicle_id) == 16  # "veh_" + 12 chars

    # Test SHA256 hex function
    hash_result = sha256_hex("test_string")
    assert isinstance(hash_result, str)
    assert len(hash_result) == 64  # SHA256 hex length


def test_package_imports():
    """Test that all core package components can be imported."""
    from core import settings
    from core.utils.hashing import generate_trip_key, generate_vehicle_id_h, sha256_hex
    from core.utils.logging import (
        get_logger,
        setup_console_logging,
        setup_development_logging,
        setup_json_file_logging,
    )

    # If we get here without import errors, the test passes
    assert True
