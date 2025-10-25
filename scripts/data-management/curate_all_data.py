#!/usr/bin/env python3
"""
Curate all available raw data dynamically.

This script discovers all available raw data files and curates them,
without hardcoding specific months or years.
"""

import sys
from pathlib import Path
from typing import List, Tuple

from batch.loader import MonthlyLoader


def discover_raw_data(data_root: Path) -> List[Tuple[str, str, Path]]:
    """
    Discover all available raw data files.

    Args:
        data_root: Root data directory

    Returns:
        List of (year, month, file_path) tuples
    """
    raw_dir = data_root / "raw" / "yellow"
    if not raw_dir.exists():
        return []

    files = []
    for year_dir in raw_dir.iterdir():
        if not year_dir.is_dir():
            continue

        year = year_dir.name
        for month_dir in year_dir.iterdir():
            if not month_dir.is_dir():
                continue

            month = month_dir.name
            parquet_file = month_dir / f"yellow_tripdata_{year}-{month}.parquet"

            if parquet_file.exists():
                files.append((year, month, parquet_file))

    # Sort by year and month
    files.sort(key=lambda x: (x[0], x[1]))
    return files


def curate_single_file(raw_file: Path, curated_file: Path) -> dict:
    """
    Curate a single raw data file.

    Args:
        raw_file: Path to raw parquet file
        curated_file: Path to output curated parquet file

    Returns:
        Dict with curation statistics
    """
    print(f"🔄 Curating: {raw_file.name}")

    # Create output directory
    curated_file.parent.mkdir(parents=True, exist_ok=True)

    # Use MonthlyLoader to curate the data
    loader = MonthlyLoader()
    stats = loader.curate_month(raw_file, curated_file)

    print(f"✅ Curation complete: {curated_file.name}")
    print(f"   - Input rows: {stats['input_rows']:,}")
    print(f"   - Output rows: {stats['output_rows']:,}")
    print(f"   - Retention rate: {stats['retention_rate']:.1f}%")
    print(f"   - File size: {stats['file_size_mb']:.1f} MB")

    return stats


def print_summary(results: List[dict]):
    """Print summary statistics for all curated files."""
    print("\n" + "=" * 80)
    print("📊 CURATION SUMMARY")
    print("=" * 80)

    successful = [r for r in results if not r.get("skipped") and not r.get("error")]
    skipped = [r for r in results if r.get("skipped")]
    failed = [r for r in results if r.get("error")]

    if successful:
        print(f"\n✅ Successfully curated {len(successful)} file(s):")
        print(
            f"\n{'File':<25} {'Input Rows':>15} {'Output Rows':>15} {'Retention':>12} {'Size (MB)':>12}"
        )
        print("-" * 80)
        for r in successful:
            print(
                f"{r['file_name']:<25} "
                f"{r['input_rows']:>15,} "
                f"{r['output_rows']:>15,} "
                f"{r['retention_rate']:>11.2f}% "
                f"{r['file_size_mb']:>11.2f}"
            )

        # Aggregate statistics
        total_input = sum(r["input_rows"] for r in successful)
        total_output = sum(r["output_rows"] for r in successful)
        avg_retention = (total_output / total_input * 100) if total_input > 0 else 0
        total_size = sum(r["file_size_mb"] for r in successful)

        print("-" * 80)
        print(
            f"{'TOTAL':<25} "
            f"{total_input:>15,} "
            f"{total_output:>15,} "
            f"{avg_retention:>11.2f}% "
            f"{total_size:>11.2f}"
        )

    if skipped:
        print(f"\n⏭️  Skipped {len(skipped)} file(s) (already curated):")
        for r in skipped:
            print(f"   - {r['file_name']}")

    if failed:
        print(f"\n❌ Failed {len(failed)} file(s):")
        for r in failed:
            print(f"   - {r['file_name']}: {r['error']}")

    print("\n" + "=" * 80)


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Curate all available raw data")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path.cwd() / "data",
        help="Root data directory (default: ./data)",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing curated files")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    print("🚀 ChillFlow Data Curation")
    print(f"   Data root: {args.data_root.resolve()}")

    # Discover raw data files
    raw_files = discover_raw_data(args.data_root)

    if not raw_files:
        print("❌ No raw data files found!")
        print(
            f"   Expected structure: {args.data_root}/raw/yellow/YYYY/MM/yellow_tripdata_YYYY-MM.parquet"
        )
        return 1

    print(f"📊 Found {len(raw_files)} raw data file(s)")

    # Process each file
    results = []
    for year, month, raw_file in raw_files:
        month_str = f"{year}-{month}"
        curated_file = args.data_root / "curated" / "yellow" / year / month / "trips_clean.parquet"

        # Check if curated file already exists
        if curated_file.exists() and not args.force:
            print(f"⏭️  Skipping {month_str} (already curated)")
            results.append(
                {
                    "skipped": True,
                    "file_name": f"{month_str}.parquet",
                    "year": year,
                    "month": month,
                }
            )
            continue

        try:
            stats = curate_single_file(raw_file, curated_file)
            stats["file_name"] = f"{month_str}.parquet"
            stats["year"] = year
            stats["month"] = month
            stats["skipped"] = False
            results.append(stats)
        except Exception as e:
            print(f"❌ Error curating {month_str}: {e}")
            results.append(
                {
                    "error": str(e),
                    "file_name": f"{month_str}.parquet",
                    "year": year,
                    "month": month,
                    "skipped": False,
                }
            )

    # Print summary
    print_summary(results)

    # Exit with error code if any failures
    failed_count = len([r for r in results if r.get("error")])
    return 1 if failed_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
