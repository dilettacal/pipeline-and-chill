#!/usr/bin/env python3
"""
NYC Yellow Taxi Data Explorer

Provides statistical analysis and exploration of NYC yellow taxi trip data.

Usage:
    uv run python scripts/explore_taxi_data.py [DATE]

    DATE format: YYYY-MM (e.g., 2025-01)
    If no date provided, analyzes the most recent month

Examples:
    uv run python scripts/explore_taxi_data.py 2025-01
    uv run python scripts/explore_taxi_data.py
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


def find_latest_file(base_path="data/raw/yellow"):
    """Find the most recent parquet file in the data directory."""
    base = Path(base_path)
    if not base.exists():
        return None

    parquet_files = list(base.rglob("*.parquet"))
    if not parquet_files:
        return None

    # Sort by modification time
    return max(parquet_files, key=lambda p: p.stat().st_mtime)


def load_data(date_str=None, base_path="data/raw/yellow"):
    """Load yellow taxi data for specified date."""
    if date_str:
        year, month = date_str.split("-")
        file_path = (
            Path(base_path) / year / month / f"yellow_tripdata_{year}-{month}.parquet"
        )
    else:
        file_path = find_latest_file(base_path)

    if not file_path or not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    print(f"📂 Loading data from: {file_path}")
    df = pd.read_parquet(file_path)
    print(f"✓ Loaded {len(df):,} rows\n")

    return df, file_path


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def basic_info(df):
    """Display basic dataset information."""
    print_section("📊 DATASET OVERVIEW")

    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print(
        f"\nDate range: {df['tpep_pickup_datetime'].min()} to {df['tpep_pickup_datetime'].max()}"
    )
    print(
        f"Duration: {(df['tpep_pickup_datetime'].max() - df['tpep_pickup_datetime'].min()).days} days"
    )

    print(f"\nColumn names (comma-separated):")
    print(f"  {', '.join(df.columns)}")

    print("\nColumn details:")
    for i, col in enumerate(df.columns, 1):
        dtype = str(df[col].dtype)  # Convert dtype to string
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df)) * 100
        print(
            f"  {i:2d}. {col:30s} ({dtype:10s}) - {null_count:7,} nulls ({null_pct:5.2f}%)"
        )


def temporal_analysis(df):
    """Analyze temporal patterns."""
    print_section("📅 TEMPORAL ANALYSIS")

    df["hour"] = df["tpep_pickup_datetime"].dt.hour
    df["day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["day_name"] = df["tpep_pickup_datetime"].dt.day_name()

    print("\n🕐 Trips by Hour of Day:")
    hourly = df.groupby("hour").size()
    for hour, count in hourly.items():
        bar_length = int(count / hourly.max() * 40)
        print(f"  {hour:2d}:00 │{'█' * bar_length} {count:,}")

    print(f"\n  Peak hour: {hourly.idxmax()}:00 with {hourly.max():,} trips")
    print(f"  Slowest hour: {hourly.idxmin()}:00 with {hourly.min():,} trips")

    print("\n📆 Trips by Day of Week:")
    daily = (
        df.groupby("day_name")
        .size()
        .reindex(
            [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]
        )
    )
    for day, count in daily.items():
        bar_length = int(count / daily.max() * 40)
        print(f"  {day:10s} │{'█' * bar_length} {count:,}")


def trip_statistics(df):
    """Analyze trip distance and duration."""
    print_section("🚗 TRIP STATISTICS")

    # Calculate trip duration if not present
    if "trip_duration_min" not in df.columns:
        df["trip_duration_min"] = (
            df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        ).dt.total_seconds() / 60

    # Filter out outliers for better statistics
    valid_trips = df[
        (df["trip_distance"] > 0)
        & (df["trip_distance"] < 100)
        & (df["trip_duration_min"] > 0)
        & (df["trip_duration_min"] < 180)
    ]

    print(f"\n📏 Trip Distance (miles) - {len(valid_trips):,} valid trips:")
    print(f"  Mean:   {valid_trips['trip_distance'].mean():.2f}")
    print(f"  Median: {valid_trips['trip_distance'].median():.2f}")
    print(f"  Std:    {valid_trips['trip_distance'].std():.2f}")
    print(f"  Min:    {valid_trips['trip_distance'].min():.2f}")
    print(f"  Max:    {valid_trips['trip_distance'].max():.2f}")

    print(f"\n⏱️  Trip Duration (minutes):")
    print(f"  Mean:   {valid_trips['trip_duration_min'].mean():.2f}")
    print(f"  Median: {valid_trips['trip_duration_min'].median():.2f}")
    print(f"  Std:    {valid_trips['trip_duration_min'].std():.2f}")
    print(f"  Min:    {valid_trips['trip_duration_min'].min():.2f}")
    print(f"  Max:    {valid_trips['trip_duration_min'].max():.2f}")

    # Average speed
    valid_trips["avg_speed_mph"] = valid_trips["trip_distance"] / (
        valid_trips["trip_duration_min"] / 60
    )
    valid_speed = valid_trips[
        (valid_trips["avg_speed_mph"] > 0) & (valid_trips["avg_speed_mph"] < 60)
    ]
    print(f"\n🚦 Average Speed (mph):")
    print(f"  Mean:   {valid_speed['avg_speed_mph'].mean():.2f}")
    print(f"  Median: {valid_speed['avg_speed_mph'].median():.2f}")


def fare_analysis(df):
    """Analyze fares and payments."""
    print_section("💰 FARE & PAYMENT ANALYSIS")

    valid_fares = df[df["fare_amount"] > 0]

    print(f"\n💵 Fare Amount (${len(valid_fares):,} valid trips):")
    print(f"  Mean:   ${valid_fares['fare_amount'].mean():.2f}")
    print(f"  Median: ${valid_fares['fare_amount'].median():.2f}")
    print(f"  Std:    ${valid_fares['fare_amount'].std():.2f}")
    print(f"  Min:    ${valid_fares['fare_amount'].min():.2f}")
    print(f"  Max:    ${valid_fares['fare_amount'].max():.2f}")

    if "total_amount" in df.columns:
        print(f"\n💳 Total Amount:")
        print(f"  Mean:   ${df['total_amount'].mean():.2f}")
        print(f"  Median: ${df['total_amount'].median():.2f}")

    if "tip_amount" in df.columns:
        print(f"\n🎁 Tips:")
        tipped = df[df["tip_amount"] > 0]
        print(f"  Trips with tips: {len(tipped):,} ({len(tipped)/len(df)*100:.1f}%)")
        if len(tipped) > 0:
            print(f"  Average tip: ${tipped['tip_amount'].mean():.2f}")
            print(
                f"  Tip % of fare: {(tipped['tip_amount'] / tipped['fare_amount'] * 100).mean():.1f}%"
            )

    if "payment_type" in df.columns:
        print(f"\n💳 Payment Types:")
        payment_map = {
            1: "Credit card",
            2: "Cash",
            3: "No charge",
            4: "Dispute",
            5: "Unknown",
            6: "Voided trip",
        }
        payment_counts = df["payment_type"].value_counts()
        for pay_type, count in payment_counts.items():
            pay_name = payment_map.get(pay_type, f"Type {pay_type}")
            pct = (count / len(df)) * 100
            print(f"  {pay_name:15s}: {count:,} ({pct:.1f}%)")


def location_analysis(df):
    """Analyze pickup and dropoff locations."""
    print_section("📍 LOCATION ANALYSIS")

    print("\n🚖 Top 10 Pickup Locations (by zone ID):")
    top_pickup = df["PULocationID"].value_counts().head(10)
    for loc_id, count in top_pickup.items():
        pct = (count / len(df)) * 100
        bar_length = int(count / top_pickup.max() * 30)
        print(f"  Zone {loc_id:3d} │{'█' * bar_length} {count:,} ({pct:.1f}%)")

    print("\n🏁 Top 10 Dropoff Locations (by zone ID):")
    top_dropoff = df["DOLocationID"].value_counts().head(10)
    for loc_id, count in top_dropoff.items():
        pct = (count / len(df)) * 100
        bar_length = int(count / top_dropoff.max() * 30)
        print(f"  Zone {loc_id:3d} │{'█' * bar_length} {count:,} ({pct:.1f}%)")

    print(f"\n📊 Unique locations:")
    print(f"  Pickup zones: {df['PULocationID'].nunique()}")
    print(f"  Dropoff zones: {df['DOLocationID'].nunique()}")


def passenger_analysis(df):
    """Analyze passenger counts."""
    print_section("👥 PASSENGER ANALYSIS")

    if "passenger_count" in df.columns:
        passenger_counts = df["passenger_count"].value_counts().sort_index()
        print(f"\n👤 Passengers per trip:")
        for count, trips in passenger_counts.items():
            if pd.notna(count) and count > 0:
                pct = (trips / len(df)) * 100
                bar_length = int(trips / passenger_counts.max() * 40)
                print(
                    f"  {int(count)} passenger(s) │{'█' * bar_length} {trips:,} ({pct:.1f}%)"
                )

        valid_passengers = df[df["passenger_count"] > 0]
        print(
            f"\n  Average passengers per trip: {valid_passengers['passenger_count'].mean():.2f}"
        )


def data_quality_check(df):
    """Check for data quality issues."""
    print_section("🔍 DATA QUALITY CHECK")

    issues = []

    # Check for negative values
    if "fare_amount" in df.columns:
        negative_fares = (df["fare_amount"] < 0).sum()
        if negative_fares > 0:
            issues.append(f"❌ Negative fares: {negative_fares:,}")

    if "trip_distance" in df.columns:
        zero_distance = (df["trip_distance"] == 0).sum()
        if zero_distance > 0:
            issues.append(
                f"⚠️  Zero distance trips: {zero_distance:,} ({zero_distance/len(df)*100:.2f}%)"
            )

    # Check for duration issues
    df["duration"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds()
    negative_duration = (df["duration"] < 0).sum()
    if negative_duration > 0:
        issues.append(
            f"❌ Negative duration (dropoff before pickup): {negative_duration:,}"
        )

    zero_duration = (df["duration"] == 0).sum()
    if zero_duration > 0:
        issues.append(
            f"⚠️  Zero duration trips: {zero_duration:,} ({zero_duration/len(df)*100:.2f}%)"
        )

    # Check for null location IDs
    null_pickup = df["PULocationID"].isna().sum()
    null_dropoff = df["DOLocationID"].isna().sum()
    if null_pickup > 0:
        issues.append(
            f"⚠️  Null pickup locations: {null_pickup:,} ({null_pickup/len(df)*100:.2f}%)"
        )
    if null_dropoff > 0:
        issues.append(
            f"⚠️  Null dropoff locations: {null_dropoff:,} ({null_dropoff/len(df)*100:.2f}%)"
        )

    if issues:
        print("\nIssues found:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("\n✅ No major data quality issues detected!")


def summary_statistics(df):
    """Print summary statistics table."""
    print_section("📈 SUMMARY STATISTICS")

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    summary = df[numeric_cols].describe()

    print("\n" + summary.to_string())


def main():
    parser = argparse.ArgumentParser(
        description="Explore NYC Yellow Taxi data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python scripts/explore_taxi_data.py 2025-01
  uv run python scripts/explore_taxi_data.py
        """,
    )
    parser.add_argument("date", nargs="?", help="Date in YYYY-MM format (optional)")
    parser.add_argument(
        "--summary-only", action="store_true", help="Show only summary statistics"
    )

    args = parser.parse_args()

    try:
        # Load data
        df, file_path = load_data(args.date)

        print("=" * 70)
        print(f"  NYC YELLOW TAXI DATA EXPLORER")
        print("=" * 70)
        print(f"File: {file_path.name}")
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if args.summary_only:
            summary_statistics(df)
        else:
            # Run all analyses
            basic_info(df)
            temporal_analysis(df)
            trip_statistics(df)
            fare_analysis(df)
            location_analysis(df)
            passenger_analysis(df)
            data_quality_check(df)

            print("\n" + "=" * 70)
            print("✅ Analysis complete!")
            print("=" * 70)
            print("\nTo see detailed summary statistics, run with --summary-only flag")

    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("\nMake sure you have downloaded the data first:")
        print("  ./scripts/data-management/download_nyc_data.sh 2025-01 2025-01")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
