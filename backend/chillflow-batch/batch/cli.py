"""
Command-line interface for ChillFlow Batch Processing Service.
"""

from datetime import datetime, timedelta
from pathlib import Path

import click
from core import get_logger, setup_development_logging

from . import BatchAggregator, BatchScheduler, BatchTripProducer, MonthlyLoader

logger = get_logger("chillflow-batch.cli")


@click.group()
@click.option("--log-level", default="INFO", help="Log level")
@click.option("--service-name", default="chillflow-batch", help="Service name for logging")
def cli(log_level: str, service_name: str):
    """ChillFlow Batch Processing Service CLI."""
    setup_development_logging(service_name)
    logger.info("ChillFlow Batch CLI started", log_level=log_level, service_name=service_name)


@cli.group()
def aggregate():
    """Aggregation commands."""


@aggregate.command()
@click.option("--incremental", is_flag=True, help="Perform incremental aggregation for yesterday")
def run(incremental: bool):
    """Run aggregation job."""
    aggregator = BatchAggregator()

    try:
        if incremental:
            # Incremental for yesterday
            yesterday = datetime.now() - timedelta(days=1)
            logger.info(
                "Starting incremental aggregation",
                target_date=yesterday.date().isoformat(),
            )
            stats = aggregator.aggregate_incremental(yesterday)
        else:
            # Default to full refresh
            logger.info("Starting full refresh aggregation")
            stats = aggregator.aggregate_full_refresh()

        click.echo(f"✅ Aggregation complete: {stats}")

    except Exception as e:
        logger.error("Aggregation failed", error=str(e))
        click.echo(f"❌ Aggregation failed: {e}")
        raise click.Abort()

    finally:
        aggregator.close()


@aggregate.command()
def status():
    """Show aggregation status."""
    aggregator = BatchAggregator()

    try:
        count = aggregator.get_kpi_count()
        date_range = aggregator.get_date_range()

        click.echo(f"📊 KPI Records: {count:,}")
        if date_range:
            click.echo(f"📅 Date Range: {date_range['min_hour']} to {date_range['max_hour']}")
        else:
            click.echo("📅 Date Range: No data")

    except Exception as e:
        logger.error("Status check failed", error=str(e))
        click.echo(f"❌ Status check failed: {e}")
        raise click.Abort()

    finally:
        aggregator.close()


@cli.group()
def process():
    """Processing commands."""


@process.command()
@click.argument("curated_path", type=click.Path(exists=True, path_type=Path))
@click.option("--batch-size", default=1000, help="Batch size for processing")
@click.option("--source", default="batch", help="Source identifier")
def trips(curated_path: Path, batch_size: int, source: str):
    """Process curated trip data."""
    producer = BatchTripProducer()

    try:
        logger.info(
            "Starting trip processing",
            file=str(curated_path),
            batch_size=batch_size,
            source=source,
        )
        stats = producer.process_month(curated_path, batch_size=batch_size, source=source)

        click.echo(f"✅ Processing complete:")
        click.echo(f"   📊 Rows read: {stats['rows_read']:,}")
        click.echo(f"   ✅ Inserted: {stats['inserted']:,}")
        click.echo(f"   ⏭️  Skipped: {stats['skipped']:,}")
        click.echo(f"   ⏱️  Duration: {stats['duration_sec']:.1f}s")

    except Exception as e:
        logger.error("Trip processing failed", error=str(e))
        click.echo(f"❌ Processing failed: {e}")
        raise click.Abort()

    finally:
        producer.close()


@cli.group()
def curate():
    """Data curation commands."""


@curate.command()
@click.argument("raw_path", type=click.Path(exists=True, path_type=Path))
@click.argument("curated_path", type=click.Path(path_type=Path))
def month(raw_path: Path, curated_path: Path):
    """Curate a month of raw data."""
    loader = MonthlyLoader()

    try:
        logger.info(
            "Starting month curation",
            raw_file=str(raw_path),
            output_file=str(curated_path),
        )
        stats = loader.curate_month(raw_path, curated_path)

        click.echo(f"✅ Curation complete:")
        click.echo(f"   📊 Input rows: {stats['input_rows']:,}")
        click.echo(f"   📊 Output rows: {stats['output_rows']:,}")
        click.echo(f"   📈 Retention: {stats['retention_rate']:.2f}%")
        click.echo(f"   📁 File size: {stats['file_size_mb']:.2f} MB")

        if stats["rejections"]:
            click.echo(f"   🚫 Rejections:")
            for rule, count in stats["rejections"].items():
                click.echo(f"      {rule}: {count:,}")

    except Exception as e:
        logger.error("Curation failed", error=str(e))
        click.echo(f"❌ Curation failed: {e}")
        raise click.Abort()


@cli.group()
def schedule():
    """Scheduling commands."""


@schedule.command()
@click.option("--check-interval", default=60, help="Check interval in seconds")
def start(check_interval: int):
    """Start the batch scheduler."""
    scheduler = BatchScheduler()

    try:
        # Set up default schedules
        scheduler.schedule_daily_aggregation(hour=2, minute=0)
        scheduler.schedule_hourly_aggregation(minute=0)
        scheduler.schedule_monthly_processing(day=1, hour=3, minute=0)

        logger.info("Starting batch scheduler", check_interval=check_interval)
        click.echo("🚀 Starting ChillFlow Batch Scheduler...")
        click.echo("   Press Ctrl+C to stop")

        scheduler.start_scheduler(check_interval=check_interval)

    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
        click.echo("\n🛑 Scheduler stopped")

    except Exception as e:
        logger.error("Scheduler failed", error=str(e))
        click.echo(f"❌ Scheduler failed: {e}")
        raise click.Abort()

    finally:
        scheduler.close()


@schedule.command()
def jobs():
    """Show scheduled jobs."""
    scheduler = BatchScheduler()

    try:
        jobs = scheduler.get_scheduled_jobs()

        if jobs:
            click.echo("📅 Scheduled Jobs:")
            for job in jobs:
                click.echo(f"   🔧 {job['job']}")
                click.echo(f"      Next run: {job['next_run']}")
                click.echo(f"      Interval: {job['interval']} {job['unit']}")
                click.echo()
        else:
            click.echo("📅 No scheduled jobs")

    except Exception as e:
        logger.error("Job listing failed", error=str(e))
        click.echo(f"❌ Job listing failed: {e}")
        raise click.Abort()

    finally:
        scheduler.close()


if __name__ == "__main__":
    cli()
