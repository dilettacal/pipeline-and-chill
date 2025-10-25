#!/bin/bash
# Sanity check for downloaded NYC Taxi data
#
# Usage:
#   ./sanity_check_data.sh [DATE]
#
# Date format: YYYY-MM (e.g., 2025-01)
# If no date provided, checks all downloaded files
#
# Examples:
#   ./sanity_check_data.sh 2025-01      # Check January 2025
#   ./sanity_check_data.sh              # Check all files

set -e

OUTPUT_BASE="data/raw/yellow"

# Check if uv is available
if ! command -v uv &> /dev/null; then
    echo "❌ Error: uv is not installed"
    echo "Install with: pip install uv"
    exit 1
fi

# Check if pandas is available in uv environment
if ! uv run python -c "import pandas" 2>/dev/null; then
    echo "❌ Error: pandas is not installed in uv environment"
    echo "Install with: uv add pandas pyarrow"
    exit 1
fi

echo "========================================"
echo "NYC Taxi Data Sanity Check"
echo "========================================"

# Function to check a single file
check_file() {
    local file_path=$1
    local filename=$(basename "$file_path")

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📊 Checking: $filename"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    uv run python - <<PY
import pandas as pd
import sys
from pathlib import Path

try:
    # Read the parquet file
    df = pd.read_parquet("$file_path")

    # Basic info
    print(f"✓ File loaded successfully")
    print(f"📏 Rows: {len(df):,}")
    print(f"📋 Columns: {len(df.columns)}")
    print(f"💾 Memory: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print()

    # Show first 12 columns
    cols = list(df.columns)
    print("📝 Columns:", cols[:12], "..." if len(cols) > 12 else "")
    print()

    # Check for key columns
    expected_cols = ['tpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'fare_amount', 'tip_amount']
    missing_cols = [col for col in expected_cols if col not in df.columns]

    if missing_cols:
        print(f"⚠️  Warning: Missing expected columns: {missing_cols}")
        print()

    # Show sample data
    if all(col in df.columns for col in expected_cols):
        print("📋 Sample data (first 5 rows):")
        print(df[expected_cols].head().to_string(index=False))
    else:
        print("📋 Sample data (first 5 rows, first 5 columns):")
        print(df.iloc[:5, :5].to_string(index=False))
    print()

    # Basic statistics
    if 'fare_amount' in df.columns:
        print(f"💰 Fare amount - Min: \${df['fare_amount'].min():.2f}, Max: \${df['fare_amount'].max():.2f}, Mean: \${df['fare_amount'].mean():.2f}")

    if 'trip_distance' in df.columns:
        print(f"🚗 Trip distance - Min: {df['trip_distance'].min():.2f}, Max: {df['trip_distance'].max():.2f}, Mean: {df['trip_distance'].mean():.2f} miles")

    # Check for null values in key columns
    if any(col in df.columns for col in expected_cols):
        print()
        print("🔍 Null value check:")
        for col in expected_cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                null_pct = (null_count / len(df)) * 100
                if null_count > 0:
                    print(f"  ⚠️  {col}: {null_count:,} nulls ({null_pct:.2f}%)")
                else:
                    print(f"  ✓ {col}: no nulls")

    sys.exit(0)

except Exception as e:
    print(f"❌ Error reading file: {e}")
    sys.exit(1)
PY

    if [ $? -eq 0 ]; then
        echo "✅ Sanity check passed"
    else
        echo "❌ Sanity check failed"
        return 1
    fi
}

# Parse arguments
if [ -n "$1" ]; then
    # Specific date provided
    DATE=$1

    # Validate date format
    if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
        echo "❌ Error: Date must be in YYYY-MM format (e.g., 2025-01)"
        exit 1
    fi

    YEAR=$(echo "$DATE" | cut -d'-' -f1)
    MONTH=$(echo "$DATE" | cut -d'-' -f2)

    FILE_PATH="$OUTPUT_BASE/$YEAR/$MONTH/yellow_tripdata_${YEAR}-${MONTH}.parquet"

    if [ ! -f "$FILE_PATH" ]; then
        echo "❌ Error: File not found: $FILE_PATH"
        echo "Have you downloaded the data yet?"
        exit 1
    fi

    check_file "$FILE_PATH"

else
    # Check all downloaded files
    FILES=$(find "$OUTPUT_BASE" -name "*.parquet" -type f 2>/dev/null | sort)

    if [ -z "$FILES" ]; then
        echo "❌ No parquet files found in $OUTPUT_BASE"
        echo "Have you downloaded the data yet?"
        exit 1
    fi

    FILE_COUNT=$(echo "$FILES" | wc -l)
    echo "Found $FILE_COUNT file(s) to check"

    CHECKED=0
    PASSED=0
    FAILED=0

    for file in $FILES; do
        if check_file "$file"; then
            PASSED=$((PASSED + 1))
        else
            FAILED=$((FAILED + 1))
        fi
        CHECKED=$((CHECKED + 1))
    done

    echo ""
    echo "========================================"
    echo "✅ Sanity check complete!"
    echo "========================================"
    echo "Files checked: $CHECKED"
    echo "Passed: $PASSED"
    echo "Failed: $FAILED"
    echo ""
fi
