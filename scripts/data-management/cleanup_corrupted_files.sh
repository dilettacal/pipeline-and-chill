#!/bin/bash
# Clean up corrupted parquet files (error responses saved as .parquet)
# This script finds and removes files that are:
# - Suspiciously small (< 1KB)
# - Contain error messages (XML/HTML errors)

set -e

OUTPUT_BASE="data/raw/yellow"

echo "========================================"
echo "Checking for corrupted files..."
echo "========================================"

# Find all .parquet files
FILES=$(find "$OUTPUT_BASE" -name "*.parquet" -type f 2>/dev/null | sort)

if [ -z "$FILES" ]; then
    echo "No parquet files found in $OUTPUT_BASE"
    exit 0
fi

CHECKED=0
CORRUPTED=0
HEALTHY=0

for file in $FILES; do
    CHECKED=$((CHECKED + 1))

    # Get file size (works on both macOS and Linux)
    FILE_SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)

    IS_CORRUPTED=0
    REASON=""

    # Check if file is suspiciously small
    if [ "$FILE_SIZE" -lt 1000 ]; then
        # Check for error patterns
        if grep -q -E "(<Error>|<html>|Access Denied|404)" "$file" 2>/dev/null; then
            IS_CORRUPTED=1
            REASON="Contains error message (${FILE_SIZE} bytes)"
        elif [ "$FILE_SIZE" -eq 0 ]; then
            IS_CORRUPTED=1
            REASON="Empty file"
        fi
    fi

    if [ "$IS_CORRUPTED" -eq 1 ]; then
        echo "❌ CORRUPTED: $(basename "$file")"
        echo "   Path: $file"
        echo "   Reason: $REASON"

        # Show first few lines if it's a text error
        if [ "$FILE_SIZE" -lt 500 ]; then
            echo "   Content preview:"
            head -3 "$file" 2>/dev/null | sed 's/^/   │ /'
        fi

        echo "   Removing..."
        rm "$file"
        CORRUPTED=$((CORRUPTED + 1))
        echo ""
    else
        HEALTHY=$((HEALTHY + 1))
    fi
done

echo "========================================"
echo "✅ Cleanup complete!"
echo "========================================"
echo "Files checked: $CHECKED"
echo "Healthy files: $HEALTHY"
echo "Corrupted files removed: $CORRUPTED"
echo ""

if [ "$CORRUPTED" -gt 0 ]; then
    echo "💡 Tip: Re-run the download script when data becomes available"
    echo "   ./scripts/data-management/download_nyc_data.sh"
fi
