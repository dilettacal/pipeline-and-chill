"""
Batch trip producer for processing curated data and writing to Postgres.

This module handles:
- Reading curated parquet files
- Generating trip keys and vehicle IDs
- Converting DataFrame rows to trip dictionaries
- Batch writing to database
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import structlog
from core import (
    CompleteTrip,
    CompleteTripSchema,
    generate_trip_key,
    generate_vehicle_id_h,
    get_database_client,
    get_logger,
    settings,
)
from core.clients.database import get_db_session
from tqdm import tqdm

logger = get_logger("chillflow-batch.producer")


class BatchTripProducer:
    """
    Processes curated trip data and writes to Postgres in batches.
    """

    def __init__(self):
        """Initialize batch processor."""
        self.db_client = get_database_client()
        self.hash_salt = settings.HASH_SALT
        logger.info(
            "Batch trip producer initialized", hash_salt_length=len(self.hash_salt)
        )

    def process_month(
        self, curated_path: Path, batch_size: int = 1000, source: str = "batch"
    ) -> Dict[str, any]:
        """
        Process a single month of curated data.

        Reads the parquet file, generates trip keys, and writes to Postgres.

        Args:
            curated_path: Path to curated parquet file
            batch_size: Number of trips to insert per batch
            source: Source identifier (default: "batch")

        Returns:
            Dict with statistics (rows_read, inserted, skipped, duration_sec)

        Raises:
            FileNotFoundError: If curated file doesn't exist
        """
        if not curated_path.exists():
            raise FileNotFoundError(f"Curated file not found: {curated_path}")

        logger.info(
            "Processing batch file",
            file_name=curated_path.name,
            batch_size=batch_size,
            source=source,
        )
        start_time = datetime.now()

        # Read curated parquet
        logger.info("Reading curated parquet file", path=str(curated_path))
        df = pd.read_parquet(curated_path, engine="pyarrow")
        rows_read = len(df)
        logger.info("Loaded curated data", rows=rows_read)

        # Convert to trip dictionaries
        logger.info("Generating trip keys and preparing data")
        trips = self._dataframe_to_trips(df, source=source)

        # Write to database in batches
        logger.info(
            "Writing to database", batch_size=batch_size, total_trips=len(trips)
        )
        stats = self._write_trips_batch(trips, batch_size=batch_size)

        # Calculate duration
        duration_sec = (datetime.now() - start_time).total_seconds()

        result = {
            "rows_read": rows_read,
            "inserted": stats["inserted"],
            "skipped": stats["skipped"],
            "duration_sec": duration_sec,
        }

        logger.info(
            "Batch processing complete",
            inserted=stats["inserted"],
            skipped=stats["skipped"],
            duration_sec=duration_sec,
            throughput_trips_per_sec=(
                len(trips) / duration_sec if duration_sec > 0 else 0
            ),
        )

        return result

    def _dataframe_to_trips(
        self, df: pd.DataFrame, source: str = "batch"
    ) -> List[Dict[str, any]]:
        """
        Convert DataFrame rows to trip dictionaries.

        Generates trip_key and vehicle_id_h for each row.

        Args:
            df: DataFrame with curated trip data
            source: Source identifier

        Returns:
            List of trip dictionaries ready for database insertion
        """
        trips = []
        now = datetime.now()

        # Use tqdm for progress bar
        for idx, row in tqdm(
            df.iterrows(), total=len(df), desc="Processing trips", unit=" trips"
        ):
            # Generate identifiers
            trip_key = generate_trip_key(
                salt=self.hash_salt,
                vendor_id=int(row["vendor_id"]),
                pickup_ts=row["pickup_ts"],
                pu_zone_id=int(row["pu_zone_id"]),
                row_offset=idx,
            )

            vehicle_id_h = generate_vehicle_id_h(
                salt=self.hash_salt,
                vendor_id=int(row["vendor_id"]),
                row_offset=idx,
            )

            # Build trip dictionary
            trip = {
                "trip_key": trip_key,
                "vendor_id": int(row["vendor_id"]),
                "vehicle_id_h": vehicle_id_h,
                "pickup_ts": row["pickup_ts"],
                "dropoff_ts": row["dropoff_ts"],
                "pu_zone_id": (
                    int(row["pu_zone_id"]) if pd.notna(row["pu_zone_id"]) else None
                ),
                "do_zone_id": (
                    int(row["do_zone_id"]) if pd.notna(row["do_zone_id"]) else None
                ),
                "passenger_count": (
                    int(row["passenger_count"])
                    if pd.notna(row["passenger_count"])
                    else None
                ),
                "rate_code": (
                    int(row["rate_code"]) if pd.notna(row["rate_code"]) else None
                ),
                "store_and_fwd_flag": (
                    row["store_and_fwd_flag"]
                    if pd.notna(row["store_and_fwd_flag"])
                    else None
                ),
                "fare_amount": (
                    float(row["fare_amount"]) if pd.notna(row["fare_amount"]) else None
                ),
                "total_amount": (
                    float(row["total_amount"])
                    if pd.notna(row["total_amount"])
                    else None
                ),
                "payment_type": (
                    int(row["payment_type"]) if pd.notna(row["payment_type"]) else None
                ),
                "tip_amount": (
                    float(row["tip_amount"]) if pd.notna(row["tip_amount"]) else None
                ),
                "tolls_amount": (
                    float(row["tolls_amount"])
                    if pd.notna(row["tolls_amount"])
                    else None
                ),
                "congestion_surcharge": (
                    float(row["congestion_surcharge"])
                    if pd.notna(row["congestion_surcharge"])
                    else None
                ),
                "airport_fee": (
                    float(row["airport_fee"]) if pd.notna(row["airport_fee"]) else None
                ),
                "distance_km": (
                    float(row["distance_km"]) if pd.notna(row["distance_km"]) else None
                ),
                "duration_min": (
                    float(row["duration_min"])
                    if pd.notna(row["duration_min"])
                    else None
                ),
                "avg_speed_kmh": (
                    float(row["avg_speed_kmh"])
                    if pd.notna(row["avg_speed_kmh"])
                    else None
                ),
                "last_update_ts": now,
                "source": source,
            }

            trips.append(trip)

        logger.info("Converted DataFrame to trips", total_trips=len(trips))
        return trips

    def _write_trips_batch(
        self, trips: List[Dict], batch_size: int = 1000
    ) -> Dict[str, int]:
        """
        Write trips to database in batches.

        Args:
            trips: List of trip dictionaries
            batch_size: Number of trips per batch

        Returns:
            Dict with statistics (inserted, skipped)
        """
        session_gen = get_db_session()
        session = next(session_gen)

        try:
            inserted = 0
            skipped = 0
            total_batches = (len(trips) + batch_size - 1) // batch_size

            logger.info(
                "Writing trips to database",
                total_trips=len(trips),
                batch_size=batch_size,
                total_batches=total_batches,
            )

            for i in range(0, len(trips), batch_size):
                batch = trips[i : i + batch_size]
                batch_num = i // batch_size + 1

                # Process batch
                batch_inserted, batch_skipped = self._process_batch(session, batch)
                inserted += batch_inserted
                skipped += batch_skipped

                # Show progress
                if batch_num % 10 == 0 or batch_num == total_batches:
                    logger.info(
                        "Batch progress",
                        batch_num=batch_num,
                        total_batches=total_batches,
                        batch_size=len(batch),
                        total_inserted=inserted,
                        total_skipped=skipped,
                    )

            session.commit()
            logger.info(
                "Database write complete",
                total_inserted=inserted,
                total_skipped=skipped,
            )

            return {"inserted": inserted, "skipped": skipped}

        finally:
            session.close()

    def _process_batch(self, session, batch: List[Dict]) -> tuple[int, int]:
        """
        Process a single batch of trips.

        Args:
            session: Database session
            batch: List of trip dictionaries

        Returns:
            Tuple of (inserted, skipped)
        """
        inserted = 0
        skipped = 0

        for trip_dict in batch:
            try:
                # Validate trip data
                trip_schema = CompleteTripSchema(**trip_dict)

                # Create SQLAlchemy model
                trip = CompleteTrip(**trip_schema.model_dump())
                session.add(trip)
                inserted += 1

            except Exception as e:
                logger.warning(
                    "Skipped invalid trip",
                    trip_key=trip_dict.get("trip_key"),
                    error=str(e),
                )
                skipped += 1

        return inserted, skipped

    def close(self):
        """Close database connections."""
        self.db_client.close()
        logger.info("Batch trip producer closed")
