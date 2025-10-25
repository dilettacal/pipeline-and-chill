#!/usr/bin/env python3
"""
Create curated data from raw NYC taxi data.

This script processes raw parquet files and creates curated versions
with derived fields, data quality rules, and standardized column names.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from batch.loader import MonthlyLoader


def create_curated_data(raw_file: Path, curated_file: Path, sample_size: int = None):
    """
    Create curated data from raw NYC taxi data.

    Args:
        raw_file: Path to raw parquet file
        curated_file: Path to output curated parquet file
        sample_size: Optional sample size (for testing)
    """
    print(f"🔄 Creating curated data from: {raw_file}")

    # Create a sample if requested
    if sample_size:
        print(f"📊 Creating sample of {sample_size:,} rows")
        df = pd.read_parquet(raw_file)
        sample_df = df.head(sample_size).copy()

        # Save sample to temp file
        temp_file = raw_file.parent / f"temp_{raw_file.name}"
        sample_df.to_parquet(temp_file, index=False)
        raw_file = temp_file

    # Use MonthlyLoader to curate the data
    loader = MonthlyLoader()
    stats = loader.curate_month(raw_file, curated_file)

    print(f"✅ Created curated data: {curated_file}")
    print(f"   - Input rows: {stats['input_rows']:,}")
    print(f"   - Output rows: {stats['output_rows']:,}")
    print(f"   - Retention rate: {stats['retention_rate']:.1f}%")
    print(f"   - File size: {stats['file_size_mb']:.1f} MB")

    # Clean up temp file if created
    if sample_size and temp_file.exists():
        temp_file.unlink()
        print(f"🧹 Cleaned up temp file: {temp_file}")

    return stats


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Create curated data from raw NYC taxi data")
    parser.add_argument("raw_file", type=Path, help="Path to raw parquet file")
    parser.add_argument("curated_file", type=Path, help="Path to output curated parquet file")
    parser.add_argument("--sample", type=int, help="Sample size for testing (optional)")

    args = parser.parse_args()

    if not args.raw_file.exists():
        print(f"❌ Raw file not found: {args.raw_file}")
        return 1

    try:
        stats = create_curated_data(args.raw_file, args.curated_file, args.sample)
        print(f"\n🎉 Successfully created curated data!")
        return 0
    except Exception as e:
        print(f"❌ Error creating curated data: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
