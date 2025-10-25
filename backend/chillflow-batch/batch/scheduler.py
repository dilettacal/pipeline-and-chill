"""
Batch scheduler for managing scheduled batch processing jobs.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import schedule
import structlog
from core import get_logger

from .aggregator import BatchAggregator
from .loader import MonthlyLoader
from .producer import BatchTripProducer

logger = get_logger("chillflow-batch.scheduler")


class BatchScheduler:
    """
    Scheduler for batch processing jobs.
    """

    def __init__(self):
        """Initialize batch scheduler."""
        self.aggregator = BatchAggregator()
        self.producer = BatchTripProducer()
        self.loader = MonthlyLoader()
        self.running = False
        logger.info("Batch scheduler initialized")

    def schedule_daily_aggregation(self, hour: int = 2, minute: int = 0):
        """
        Schedule daily aggregation job.

        Args:
            hour: Hour to run (24-hour format)
            minute: Minute to run
        """
        schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(self._run_daily_aggregation)
        logger.info("Scheduled daily aggregation", hour=hour, minute=minute)

    def schedule_hourly_aggregation(self, minute: int = 0):
        """
        Schedule hourly aggregation job.

        Args:
            minute: Minute to run each hour
        """
        schedule.every().hour.at(f":{minute:02d}").do(self._run_hourly_aggregation)
        logger.info("Scheduled hourly aggregation", minute=minute)

    def schedule_monthly_processing(self, day: int = 1, hour: int = 3, minute: int = 0):
        """
        Schedule monthly processing job.

        Args:
            day: Day of month to run
            hour: Hour to run
            minute: Minute to run
        """
        # Schedule for the 1st of each month
        schedule.every().month.do(self._run_monthly_processing)
        logger.info("Scheduled monthly processing", day=day, hour=hour, minute=minute)

    def _run_daily_aggregation(self):
        """Run daily aggregation job."""
        logger.info("Starting daily aggregation")
        try:
            # Run incremental aggregation for yesterday
            yesterday = datetime.now() - timedelta(days=1)
            stats = self.aggregator.aggregate_incremental(yesterday, lookback_hours=24)
            logger.info("Daily aggregation complete", stats=stats)
        except Exception as e:
            logger.error("Daily aggregation failed", error=str(e))

    def _run_hourly_aggregation(self):
        """Run hourly aggregation job."""
        logger.info("Starting hourly aggregation")
        try:
            # Run incremental aggregation for the last hour
            now = datetime.now()
            stats = self.aggregator.aggregate_incremental(now, lookback_hours=1)
            logger.info("Hourly aggregation complete", stats=stats)
        except Exception as e:
            logger.error("Hourly aggregation failed", error=str(e))

    def _run_monthly_processing(self):
        """Run monthly processing job."""
        logger.info("Starting monthly processing")
        try:
            # This would typically process the previous month's data
            # For now, just log that it would run
            logger.info("Monthly processing would run here")
        except Exception as e:
            logger.error("Monthly processing failed", error=str(e))

    def run_pending_jobs(self):
        """Run all pending scheduled jobs."""
        pending_jobs = schedule.get_jobs()
        if pending_jobs:
            logger.info("Running pending jobs", count=len(pending_jobs))
            schedule.run_pending()
        else:
            logger.debug("No pending jobs")

    def start_scheduler(self, check_interval: int = 60):
        """
        Start the scheduler loop.

        Args:
            check_interval: Seconds between job checks
        """
        logger.info("Starting batch scheduler", check_interval=check_interval)
        self.running = True

        while self.running:
            try:
                self.run_pending_jobs()
                time.sleep(check_interval)
            except KeyboardInterrupt:
                logger.info("Scheduler interrupted by user")
                break
            except Exception as e:
                logger.error("Scheduler error", error=str(e))
                time.sleep(check_interval)

        logger.info("Batch scheduler stopped")

    def stop_scheduler(self):
        """Stop the scheduler."""
        logger.info("Stopping batch scheduler")
        self.running = False

    def get_scheduled_jobs(self) -> List[Dict]:
        """
        Get list of scheduled jobs.

        Returns:
            List of job information dictionaries
        """
        jobs = []
        for job in schedule.get_jobs():
            jobs.append(
                {
                    "job": str(job.job_func),
                    "next_run": job.next_run.isoformat() if job.next_run else None,
                    "interval": job.interval,
                    "unit": job.unit,
                }
            )
        return jobs

    def clear_jobs(self):
        """Clear all scheduled jobs."""
        schedule.clear()
        logger.info("Cleared all scheduled jobs")

    def close(self):
        """Close all components."""
        self.stop_scheduler()
        self.aggregator.close()
        self.producer.close()
        logger.info("Batch scheduler closed")
