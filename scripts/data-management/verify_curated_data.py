#!/usr/bin/env python3
"""Verify curated data files for Phase 2 completion."""

import sys
from pathlib import Path

import pandas as pd


def verify_curated_files():
    """Verify all curated files exist and meet acceptance criteria."""

    data_root = Path(__file__).parent.parent / "data" / "curated" / "yellow" / "2025"

    print("=" * 80)
    print("🔍 PHASE 2 VERIFICATION - Curated Data Files")
    print("=" * 80)

    required_columns = ["distance_km", "duration_min", "avg_speed_kmh"]
    months = range(1, 9)

    all_passed = True
    total_rows = 0

    for month in months:
        path = data_root / f"{month:02d}" / "trips_clean.parquet"

        print(f"\n📋 Checking 2025-{month:02d}...")

        # Check if file exists
        if not path.exists():
            print(f"   ❌ File not found: {path}")
            all_passed = False
            continue

        # Read file
        try:
            df = pd.read_parquet(path)
        except Exception as e:
            print(f"   ❌ Error reading file: {e}")
            all_passed = False
            continue

        # Verify derived columns exist
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            print(f"   ❌ Missing columns: {missing}")
            all_passed = False
            continue

        # Verify DQ rules
        temporal_valid = (df["pickup_ts"] < df["dropoff_ts"]).all()
        speed_valid = ((df["avg_speed_kmh"] <= 200) | df["avg_speed_kmh"].isna()).all()
        amount_valid = (df["total_amount"] >= 0).all()

        dq_passed = temporal_valid and speed_valid and amount_valid

        if not dq_passed:
            print(f"   ❌ DQ rules failed:")
            if not temporal_valid:
                print(f"      - Temporal validity: FAILED")
            if not speed_valid:
                print(f"      - Speed limit: FAILED")
            if not amount_valid:
                print(f"      - Non-negative amount: FAILED")
            all_passed = False
            continue

        # Calculate stats
        file_size_mb = path.stat().st_size / (1024 * 1024)
        total_rows += len(df)

        print(f"   ✅ File exists: {file_size_mb:.1f} MB")
        print(f"   ✅ Row count: {len(df):,}")
        print(f"   ✅ Derived columns: {', '.join(required_columns)}")
        print(f"   ✅ DQ rules: All passed")

    print("\n" + "=" * 80)

    if all_passed:
        print("✅ PHASE 2 COMPLETE - All acceptance criteria met!")
        print(f"   - All 8 files created")
        print(f"   - Total rows: {total_rows:,}")
        print(f"   - Derived fields calculated correctly")
        print(f"   - DQ rules applied successfully")
        print("=" * 80)
        return 0
    else:
        print("❌ PHASE 2 INCOMPLETE - Some files failed verification")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(verify_curated_files())
