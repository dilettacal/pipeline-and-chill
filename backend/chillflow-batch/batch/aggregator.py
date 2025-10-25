"""
Batch aggregator for computing zone hourly KPIs.

Reads from stg.complete_trip and writes aggregated metrics to mart.zone_hourly_kpis.
"""

from datetime import datetime, timedelta
from typing import Dict, Optional

import structlog
from core import CompleteTrip, Zone, get_database_client, get_logger
from core.clients.database import get_db_session
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

logger = get_logger("chillflow-batch.aggregator")


class BatchAggregator:
    """
    Computes and manages hourly zone KPI aggregations.
    """

    def __init__(self):
        """Initialize KPI aggregator."""
        self.db_client = get_database_client()
        logger.info("Batch aggregator initialized")

    def get_session(self) -> Session:
        """Get a new database session."""
        session_gen = get_db_session()
        return next(session_gen)

    def aggregate_full_refresh(self) -> Dict[str, int]:
        """
        Perform full refresh of all KPIs.

        Recomputes all zone-hour aggregations from scratch.

        Returns:
            Dict with statistics: {"hours_processed": count, "zones_affected": count}
        """
        logger.info("Starting full refresh of zone hourly KPIs")

        session = self.get_session()
        try:
            # Get date range from data
            result = session.execute(
                text(
                    """
                SELECT
                    MIN(pickup_ts) as min_ts,
                    MAX(pickup_ts) as max_ts,
                    COUNT(*) as total_trips
                FROM stg.complete_trip
                """
                )
            )
            row = result.fetchone()

            if not row or not row.min_ts:
                logger.warning("No trips found in stg.complete_trip")
                return {"hours_processed": 0, "zones_affected": 0}

            min_ts, max_ts, total_trips = row.min_ts, row.max_ts, row.total_trips
            logger.info(
                "Found trips for aggregation",
                total_trips=total_trips,
                min_ts=min_ts.isoformat(),
                max_ts=max_ts.isoformat(),
            )

            # Compute and upsert aggregations
            stats = self._compute_and_upsert_range(
                session=session,
                start_ts=min_ts,
                end_ts=max_ts + timedelta(hours=1),  # Include the last hour
            )

            logger.info(
                "Full refresh complete",
                hours_processed=stats["hours_processed"],
                zones_affected=stats["zones_affected"],
            )
            return stats

        finally:
            session.close()

    def aggregate_incremental(
        self, target_date: datetime, lookback_hours: int = 24
    ) -> Dict[str, int]:
        """
        Perform incremental aggregation for a specific date.

        Only recomputes hours within the lookback window from target_date.

        Args:
            target_date: Date to aggregate (will process full day)
            lookback_hours: How many hours before target_date to include

        Returns:
            Dict with statistics: {"hours_processed": count, "zones_affected": count}
        """
        # Start from beginning of target_date
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts = start_of_day - timedelta(hours=lookback_hours)
        end_ts = start_of_day + timedelta(days=1)

        logger.info(
            "Starting incremental aggregation",
            target_date=target_date.date().isoformat(),
            start_ts=start_ts.isoformat(),
            end_ts=end_ts.isoformat(),
            lookback_hours=lookback_hours,
        )

        session = self.get_session()
        try:
            stats = self._compute_and_upsert_range(
                session=session, start_ts=start_ts, end_ts=end_ts
            )

            logger.info(
                "Incremental aggregation complete",
                hours_processed=stats["hours_processed"],
                zones_affected=stats["zones_affected"],
            )
            return stats

        finally:
            session.close()

    def aggregate_backfill(self, start_date: datetime, end_date: datetime) -> Dict[str, int]:
        """
        Backfill aggregations for a specific date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            Dict with statistics: {"hours_processed": count, "zones_affected": count}
        """
        # Start from beginning of start_date
        start_ts = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        # End at end of end_date
        end_ts = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        logger.info(
            "Starting backfill aggregation",
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
            start_ts=start_ts.isoformat(),
            end_ts=end_ts.isoformat(),
        )

        session = self.get_session()
        try:
            stats = self._compute_and_upsert_range(
                session=session, start_ts=start_ts, end_ts=end_ts
            )

            logger.info(
                "Backfill complete",
                hours_processed=stats["hours_processed"],
                zones_affected=stats["zones_affected"],
            )
            return stats

        finally:
            session.close()

    def _compute_and_upsert_range(
        self, session: Session, start_ts: datetime, end_ts: datetime
    ) -> Dict[str, int]:
        """
        Compute aggregations for a time range and upsert to mart table.

        Args:
            session: Database session
            start_ts: Start timestamp (inclusive)
            end_ts: End timestamp (exclusive)

        Returns:
            Dict with statistics
        """
        # Build aggregation query
        agg_query = text(
            """
        SELECT
            pu_zone_id AS zone_id,
            DATE_TRUNC('hour', pickup_ts) AS hour_ts,
            COUNT(*) AS trips,
            AVG(fare_amount) AS avg_fare,
            AVG(tip_amount) AS avg_tip,
            AVG(
                CASE
                    WHEN EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts)) > 0
                    THEN (trip_distance * 1.60934) / (EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts)) / 3600)
                    ELSE NULL
                END
            ) AS avg_speed_kmh,
            AVG(trip_distance * 1.60934) AS avg_distance_km,
            AVG(EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts)) / 60) AS avg_duration_min,
            100.0 * SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct_card,
            COUNT(DISTINCT vehicle_id_h) AS unique_vehicles,
            NOW() AS created_at
        FROM stg.complete_trip
        WHERE pickup_ts >= :start_ts
          AND pickup_ts < :end_ts
          AND pu_zone_id IS NOT NULL
        GROUP BY pu_zone_id, DATE_TRUNC('hour', pickup_ts)
        ORDER BY hour_ts, zone_id
        """
        )

        # Execute aggregation
        logger.info(
            "Executing aggregation query",
            start_ts=start_ts.isoformat(),
            end_ts=end_ts.isoformat(),
        )
        result = session.execute(agg_query, {"start_ts": start_ts, "end_ts": end_ts})
        rows = result.fetchall()

        if not rows:
            logger.warning(
                "No trips found in range",
                start_ts=start_ts.isoformat(),
                end_ts=end_ts.isoformat(),
            )
            return {"hours_processed": 0, "zones_affected": 0}

        logger.info("Computed zone-hour aggregations", count=len(rows))

        # Convert to dictionaries for bulk upsert
        kpi_dicts = []
        zones_seen = set()
        hours_seen = set()

        for row in rows:
            kpi_dict = {
                "zone_id": row.zone_id,
                "hour_ts": row.hour_ts,
                "trips": row.trips,
                "avg_fare": row.avg_fare,
                "avg_tip": row.avg_tip,
                "avg_speed_kmh": row.avg_speed_kmh,
                "avg_distance_km": row.avg_distance_km,
                "avg_duration_min": row.avg_duration_min,
                "pct_card": row.pct_card,
                "outlier_rate": None,  # Optional: track filtered rows
                "unique_vehicles": row.unique_vehicles,
                "created_at": row.created_at,
            }
            kpi_dicts.append(kpi_dict)
            zones_seen.add(row.zone_id)
            hours_seen.add(row.hour_ts)

        # Bulk upsert using PostgreSQL's INSERT ... ON CONFLICT
        # Process in batches to avoid parameter limit issues
        batch_size = 500  # PostgreSQL can handle ~500 records per INSERT
        total_upserted = 0
        total_batches = (len(kpi_dicts) + batch_size - 1) // batch_size

        logger.info(
            "Upserting KPI records",
            total_records=len(kpi_dicts),
            batch_size=batch_size,
            total_batches=total_batches,
        )

        for i in range(0, len(kpi_dicts), batch_size):
            batch = kpi_dicts[i : i + batch_size]
            batch_num = i // batch_size + 1

            # Note: This would need a proper KPI model - for now we'll use raw SQL
            # In a real implementation, you'd create a ZoneHourlyKPI model
            stmt = text(
                """
                INSERT INTO mart.zone_hourly_kpis
                (zone_id, hour_ts, trips, avg_fare, avg_tip, avg_speed_kmh,
                 avg_distance_km, avg_duration_min, pct_card, unique_vehicles, created_at)
                VALUES (:zone_id, :hour_ts, :trips, :avg_fare, :avg_tip, :avg_speed_kmh,
                        :avg_distance_km, :avg_duration_min, :pct_card, :unique_vehicles, :created_at)
                ON CONFLICT (zone_id, hour_ts)
                DO UPDATE SET
                    trips = EXCLUDED.trips,
                    avg_fare = EXCLUDED.avg_fare,
                    avg_tip = EXCLUDED.avg_tip,
                    avg_speed_kmh = EXCLUDED.avg_speed_kmh,
                    avg_distance_km = EXCLUDED.avg_distance_km,
                    avg_duration_min = EXCLUDED.avg_duration_min,
                    pct_card = EXCLUDED.pct_card,
                    unique_vehicles = EXCLUDED.unique_vehicles,
                    created_at = EXCLUDED.created_at
            """
            )

            for kpi_dict in batch:
                session.execute(stmt, kpi_dict)
            total_upserted += len(batch)

            # Show progress every 50 batches or on last batch
            if batch_num % 50 == 0 or batch_num == total_batches:
                logger.info(
                    "Batch progress",
                    batch_num=batch_num,
                    total_batches=total_batches,
                    records_upserted=total_upserted,
                )

        session.commit()

        logger.info(
            "Upserted KPI records",
            total_upserted=total_upserted,
            zones_affected=len(zones_seen),
            hours_processed=len(hours_seen),
        )

        return {
            "hours_processed": len(hours_seen),
            "zones_affected": len(zones_seen),
        }

    def get_kpi_count(self) -> int:
        """
        Get count of KPI records in the mart table.

        Returns:
            Count of zone-hour KPI records
        """
        session = self.get_session()
        try:
            result = session.execute(text("SELECT COUNT(*) FROM mart.zone_hourly_kpis"))
            count = result.scalar()
            return count or 0
        finally:
            session.close()

    def get_date_range(self) -> Optional[Dict[str, datetime]]:
        """
        Get the date range of KPIs in the mart table.

        Returns:
            Dict with min_hour and max_hour, or None if empty
        """
        session = self.get_session()
        try:
            result = session.execute(
                text(
                    """
                SELECT
                    MIN(hour_ts) as min_hour,
                    MAX(hour_ts) as max_hour
                FROM mart.zone_hourly_kpis
                """
                )
            )
            row = result.fetchone()

            if not row or not row.min_hour:
                return None

            return {"min_hour": row.min_hour, "max_hour": row.max_hour}

        finally:
            session.close()

    def close(self):
        """Close database connections."""
        self.db_client.close()
        logger.info("Batch aggregator closed")
