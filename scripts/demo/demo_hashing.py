#!/usr/bin/env python3
"""
Demo script for trip key and vehicle ID generation.

Shows how to use the hashing functions with real NYC Taxi data.
"""

import sys
from datetime import datetime
from pathlib import Path

# Add core-utils to path
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "core-utils"))

from fluxframe.hashing import generate_trip_key, generate_vehicle_id_h
from fluxframe.settings import settings


def demo_basic_usage():
    """Demonstrate basic usage of hashing functions."""
    print("=" * 80)
    print("🔑 DEMO: Trip Key & Vehicle ID Generation")
    print("=" * 80)
    
    # Example trip data
    salt = settings.HASH_SALT
    vendor_id = 2
    pickup_ts = datetime(2025, 1, 1, 0, 18, 38)
    pu_zone_id = 229
    row_offset = 0
    
    print(f"\n📋 Input Data:")
    print(f"   Salt: {salt}")
    print(f"   Vendor ID: {vendor_id}")
    print(f"   Pickup Time: {pickup_ts}")
    print(f"   Pickup Zone: {pu_zone_id}")
    print(f"   Row Offset: {row_offset}")
    
    # Generate trip key
    trip_key = generate_trip_key(salt, vendor_id, pickup_ts, pu_zone_id, row_offset)
    print(f"\n🔑 Generated Trip Key:")
    print(f"   {trip_key}")
    print(f"   Length: {len(trip_key)} characters (SHA-256)")
    
    # Generate vehicle ID
    vehicle_id = generate_vehicle_id_h(salt, vendor_id, row_offset)
    print(f"\n🚕 Generated Vehicle ID:")
    print(f"   {vehicle_id}")
    print(f"   Format: veh_<12-char-hash>")


def demo_determinism():
    """Demonstrate determinism (same inputs → same outputs)."""
    print("\n" + "=" * 80)
    print("🔄 DEMO: Determinism Test")
    print("=" * 80)
    
    salt = "test-salt"
    vendor_id = 1
    pickup_ts = datetime(2025, 5, 1, 14, 23, 0)
    pu_zone_id = 161
    row_offset = 0
    
    # Generate 5 times
    print("\nGenerating trip key 5 times with same inputs...")
    keys = []
    for i in range(5):
        key = generate_trip_key(salt, vendor_id, pickup_ts, pu_zone_id, row_offset)
        keys.append(key)
        print(f"   Attempt {i+1}: {key[:16]}...")
    
    # Verify all identical
    if len(set(keys)) == 1:
        print("\n   ✅ All 5 keys are IDENTICAL (deterministic!)")
    else:
        print("\n   ❌ Keys differ (not deterministic)")


def demo_uniqueness():
    """Demonstrate uniqueness (different inputs → different outputs)."""
    print("\n" + "=" * 80)
    print("🆔 DEMO: Uniqueness Test")
    print("=" * 80)
    
    salt = "test-salt"
    pickup_ts = datetime(2025, 5, 1, 14, 23, 0)
    
    print("\nGenerating keys for different trips:")
    
    # Different vendor
    key1 = generate_trip_key(salt, vendor_id=1, pickup_ts=pickup_ts, pu_zone_id=161, row_offset=0)
    print(f"   Vendor 1, Zone 161, Offset 0: {key1[:16]}...")
    
    # Different vendor
    key2 = generate_trip_key(salt, vendor_id=2, pickup_ts=pickup_ts, pu_zone_id=161, row_offset=0)
    print(f"   Vendor 2, Zone 161, Offset 0: {key2[:16]}...")
    
    # Different zone
    key3 = generate_trip_key(salt, vendor_id=1, pickup_ts=pickup_ts, pu_zone_id=237, row_offset=0)
    print(f"   Vendor 1, Zone 237, Offset 0: {key3[:16]}...")
    
    # Different offset
    key4 = generate_trip_key(salt, vendor_id=1, pickup_ts=pickup_ts, pu_zone_id=161, row_offset=1)
    print(f"   Vendor 1, Zone 161, Offset 1: {key4[:16]}...")
    
    if len({key1, key2, key3, key4}) == 4:
        print("\n   ✅ All 4 keys are UNIQUE")
    else:
        print("\n   ❌ Some keys are identical (collision!)")


def demo_vehicle_fleet():
    """Demonstrate vehicle fleet reuse."""
    print("\n" + "=" * 80)
    print("🚖 DEMO: Vehicle Fleet Reuse")
    print("=" * 80)
    
    salt = "test-salt"
    vendor_id = 1
    fleet_size = 100
    
    print(f"\nSimulating fleet of {fleet_size} vehicles...")
    print("Vehicle IDs should repeat after every 100 trips:\n")
    
    # Show first 5 vehicles
    print("   First 5 trips:")
    for i in range(5):
        vid = generate_vehicle_id_h(salt, vendor_id, row_offset=i, fleet_size=fleet_size)
        print(f"      Row {i:3d}: {vid}")
    
    # Show vehicles at offset+100 (should match first 5)
    print(f"\n   After {fleet_size} trips (should match above):")
    for i in range(5):
        vid = generate_vehicle_id_h(salt, vendor_id, row_offset=i+fleet_size, fleet_size=fleet_size)
        print(f"      Row {i+100:3d}: {vid}")
    
    # Verify match
    vid_0 = generate_vehicle_id_h(salt, vendor_id, row_offset=0, fleet_size=fleet_size)
    vid_100 = generate_vehicle_id_h(salt, vendor_id, row_offset=100, fleet_size=fleet_size)
    
    if vid_0 == vid_100:
        print(f"\n   ✅ Vehicle at row 0 and row 100 are SAME (fleet reuse working!)")
    else:
        print(f"\n   ❌ Vehicle at row 0 and row 100 differ")


def demo_collision_test():
    """Test for hash collisions."""
    print("\n" + "=" * 80)
    print("💥 DEMO: Collision Test")
    print("=" * 80)
    
    salt = "test-salt"
    vendor_id = 1
    pickup_ts = datetime(2025, 5, 1, 14, 23, 0)
    pu_zone_id = 161
    
    print("\nGenerating 10,000 trip keys with different row offsets...")
    keys = set()
    for i in range(10_000):
        key = generate_trip_key(salt, vendor_id, pickup_ts, pu_zone_id, row_offset=i)
        keys.add(key)
    
    collision_count = 10_000 - len(keys)
    collision_rate = collision_count / 10_000 * 100
    
    print(f"   Generated: 10,000 keys")
    print(f"   Unique: {len(keys):,} keys")
    print(f"   Collisions: {collision_count}")
    print(f"   Collision rate: {collision_rate:.4f}%")
    
    if collision_rate == 0:
        print("\n   ✅ ZERO collisions (perfect!)")
    else:
        print(f"\n   ⚠️  {collision_count} collisions detected")


def main():
    """Run all demos."""
    demo_basic_usage()
    demo_determinism()
    demo_uniqueness()
    demo_vehicle_fleet()
    demo_collision_test()
    
    print("\n" + "=" * 80)
    print("✅ Demo Complete!")
    print("=" * 80)
    print("\n💡 These functions are now ready for use in:")
    print("   - Phase 5: Streaming pipeline (May-Aug data)")
    print("   - Phase 6: Batch pipeline (Jan-Apr data)")
    print("\n")


if __name__ == "__main__":
    main()

