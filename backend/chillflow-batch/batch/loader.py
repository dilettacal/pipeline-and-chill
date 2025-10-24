"""
Monthly loader for data curation and processing.

Transforms raw parquet to curated with DQ rules and derived fields.
"""

import logging
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import structlog
from core import get_logger

logger = get_logger("chillflow-batch.loader")


def compute_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived fields required for curated dataset.

    Derived fields:
    - distance_km: trip_distance * 1.60934 (miles to km)
    - duration_min: (dropoff_ts - pickup_ts) in minutes
    - avg_speed_kmh: distance_km / (duration_min / 60)

    Args:
        df: Raw DataFrame with tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance

    Returns:
        DataFrame with additional derived columns
    """
    logger.info("Computing derived fields")

    # Convert distance from miles to km
    df["distance_km"] = df["trip_distance"] * 1.60934

    # Calculate duration in minutes
    df["duration_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Calculate average speed (km/h)
    # Avoid division by zero by replacing with NaN
    df["avg_speed_kmh"] = df["distance_km"] / (df["duration_min"] / 60)
    df.loc[df["duration_min"] == 0, "avg_speed_kmh"] = pd.NA

    logger.info(
        "Derived fields computed",
        distance_km_min=df["distance_km"].min(),
        distance_km_max=df["distance_km"].max(),
        duration_min_min=df["duration_min"].min(),
        duration_min_max=df["duration_min"].max(),
        avg_speed_min=df["avg_speed_kmh"].min(),
        avg_speed_max=df["avg_speed_kmh"].max(),
    )

    return df


def apply_dq_rules(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Apply data quality rules to filter out invalid trips.

    DQ Rules:
    1. Temporal validity: Drop rows where pickup_ts >= dropoff_ts
    2. Speed limit: Drop rows where avg_speed_kmh > 200 (unrealistic)
    3. Non-negative amounts: Drop rows where total_amount < 0

    Args:
        df: DataFrame with pickup_ts, dropoff_ts, avg_speed_kmh, total_amount

    Returns:
        Tuple of (filtered DataFrame, dict of rejection counts by rule)
    """
    logger.info("Applying DQ rules")

    initial_count = len(df)
    rejections = {}

    # Rule 1: Temporal validity (pickup < dropoff)
    temporal_valid = df["tpep_pickup_datetime"] < df["tpep_dropoff_datetime"]
    rejections["temporal_invalid"] = (~temporal_valid).sum()
    df = df[temporal_valid]
    logger.info(
        "Rule 1 (temporal validity) applied", rejected=rejections["temporal_invalid"]
    )

    # Rule 2: Speed limit (avg_speed_kmh <= 200)
    # Handle NaN values (trips with 0 duration)
    speed_valid = (df["avg_speed_kmh"] <= 200) | df["avg_speed_kmh"].isna()
    rejections["speed_invalid"] = (~speed_valid).sum()
    df = df[speed_valid]
    logger.info("Rule 2 (speed limit) applied", rejected=rejections["speed_invalid"])

    # Rule 3: Non-negative amounts (total_amount >= 0)
    amount_valid = df["total_amount"] >= 0
    rejections["negative_amount"] = (~amount_valid).sum()
    df = df[amount_valid]
    logger.info(
        "Rule 3 (non-negative amount) applied", rejected=rejections["negative_amount"]
    )

    final_count = len(df)
    total_rejected = initial_count - final_count
    retention_rate = (final_count / initial_count * 100) if initial_count > 0 else 0

    logger.info(
        "DQ rules applied",
        initial_count=initial_count,
        final_count=final_count,
        total_rejected=total_rejected,
        retention_rate=retention_rate,
    )

    return df, rejections


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to match curated schema naming conventions.

    Maps TLC raw column names to our standardized snake_case names.

    Args:
        df: Raw DataFrame with TLC column names

    Returns:
        DataFrame with standardized column names
    """
    logger.info("Standardizing column names")

    rename_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_ts",
        "tpep_dropoff_datetime": "dropoff_ts",
        "passenger_count": "passenger_count",  # already snake_case
        "trip_distance": "trip_distance",  # keep original for reference
        "RatecodeID": "rate_code",
        "store_and_fwd_flag": "store_and_fwd_flag",  # already snake_case
        "PULocationID": "pu_zone_id",
        "DOLocationID": "do_zone_id",
        "payment_type": "payment_type",  # already snake_case
        "fare_amount": "fare_amount",  # already snake_case
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",  # already snake_case
        "tolls_amount": "tolls_amount",  # already snake_case
        "improvement_surcharge": "improvement_surcharge",  # already snake_case
        "total_amount": "total_amount",  # already snake_case
        "congestion_surcharge": "congestion_surcharge",  # already snake_case
        "Airport_fee": "airport_fee",
        "cbd_congestion_fee": "cbd_congestion_fee",  # new field as of 2025
    }

    df = df.rename(columns=rename_map)
    logger.info("Column names standardized", columns_renamed=len(rename_map))

    return df


class MonthlyLoader:
    """
    Monthly data loader for curating raw taxi data.
    """

    def __init__(self):
        """Initialize monthly loader."""
        logger.info("Monthly loader initialized")

    def curate_month(self, raw_path: Path, curated_path: Path) -> Dict[str, any]:
        """
        Curate a single month of raw taxi data.

        Pipeline:
        1. Read raw parquet file
        2. Compute derived fields (distance_km, duration_min, avg_speed_kmh)
        3. Apply DQ rules (temporal validity, speed limit, non-negative amounts)
        4. Standardize column names
        5. Write curated parquet file

        Args:
            raw_path: Path to raw parquet file
            curated_path: Path to output curated parquet file

        Returns:
            Dict with statistics (input_rows, output_rows, retention_rate, rejections)

        Raises:
            FileNotFoundError: If raw file doesn't exist
        """
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw file not found: {raw_path}")

        logger.info(
            "Starting month curation",
            raw_file=raw_path.name,
            output_file=curated_path.name,
        )

        # Step 1: Read raw parquet
        logger.info("Reading raw parquet file", path=str(raw_path))
        df = pd.read_parquet(raw_path, engine="pyarrow")
        input_rows = len(df)
        logger.info("Loaded raw data", rows=input_rows)

        # Step 2: Compute derived fields
        df = compute_derived_fields(df)

        # Step 3: Apply DQ rules
        df, rejections = apply_dq_rules(df)
        output_rows = len(df)

        # Step 4: Standardize column names
        df = standardize_column_names(df)

        # Step 5: Write curated parquet
        curated_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(curated_path, engine="pyarrow", index=False)
        file_size_mb = curated_path.stat().st_size / (1024 * 1024)
        logger.info(
            "Written curated parquet", path=str(curated_path), size_mb=file_size_mb
        )

        # Calculate statistics
        retention_rate = (output_rows / input_rows * 100) if input_rows > 0 else 0

        stats = {
            "input_rows": input_rows,
            "output_rows": output_rows,
            "retention_rate": retention_rate,
            "rejections": rejections,
            "file_size_mb": file_size_mb,
        }

        logger.info(
            "Month curation complete",
            input_rows=input_rows,
            output_rows=output_rows,
            retention_rate=retention_rate,
            file_size_mb=file_size_mb,
        )

        return stats
