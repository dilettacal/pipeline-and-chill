#!/bin/bash
# Download NYC Yellow Taxi Trip Data
# Also downloads data dictionary, zone lookup, and zone shapefiles
# 
# Usage: 
#   ./download_nyc_data.sh [START_DATE] [END_DATE]
#   
# Date format: YYYY-MM (e.g., 2024-10 for October 2024)
#
# Examples:
#   ./download_nyc_data.sh 2024-12 2025-03  # Dec 2024 to Mar 2025
#   ./download_nyc_data.sh 2025-01 2025-06  # Jan-June 2025
#   ./download_nyc_data.sh                   # Defaults to 2025-01 to 2025-10

set -e  # Exit on error

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
OUTPUT_BASE="data/raw/yellow"

# Parse arguments
START_DATE="${1:-2025-01}"
END_DATE="${2:-2025-10}"

# Validate date format
if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "❌ Error: START_DATE must be in YYYY-MM format (e.g., 2025-01)"
    exit 1
fi

if ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "❌ Error: END_DATE must be in YYYY-MM format (e.g., 2025-12)"
    exit 1
fi

# Extract year and month
START_YEAR=$(echo "$START_DATE" | cut -d'-' -f1)
START_MONTH=$(echo "$START_DATE" | cut -d'-' -f2)
END_YEAR=$(echo "$END_DATE" | cut -d'-' -f1)
END_MONTH=$(echo "$END_DATE" | cut -d'-' -f2)

# Remove leading zeros for comparison
START_MONTH_NUM=$((10#$START_MONTH))
END_MONTH_NUM=$((10#$END_MONTH))

# Validate month range
if [ "$START_MONTH_NUM" -lt 1 ] || [ "$START_MONTH_NUM" -gt 12 ]; then
    echo "❌ Error: Month in START_DATE must be between 01 and 12"
    exit 1
fi

if [ "$END_MONTH_NUM" -lt 1 ] || [ "$END_MONTH_NUM" -gt 12 ]; then
    echo "❌ Error: Month in END_DATE must be between 01 and 12"
    exit 1
fi

# Validate date range (start must be before or equal to end)
START_NUM=$((START_YEAR * 12 + START_MONTH_NUM))
END_NUM=$((END_YEAR * 12 + END_MONTH_NUM))

if [ "$START_NUM" -gt "$END_NUM" ]; then
    echo "❌ Error: START_DATE ($START_DATE) cannot be after END_DATE ($END_DATE)"
    exit 1
fi

# Create base directories
mkdir -p "$OUTPUT_BASE"
mkdir -p "data/raw/dictionary"
mkdir -p "data/raw/zones"
mkdir -p "data/raw/zones/shapes"

echo "========================================"
echo "Downloading reference data..."
echo "========================================"

# Download data dictionary (PDF)
DICT_FILE="data/raw/dictionary/data_dictionary_trip_records_yellow.pdf"
if [ -f "$DICT_FILE" ]; then
    echo "⏭️  Data dictionary already exists, skipping..."
else
    echo "📄 Downloading data dictionary..."
    curl -L -o "$DICT_FILE" \
         "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf"
    echo "✓ Saved to: $DICT_FILE"
fi

# Download taxi zone lookup table (CSV)
ZONE_LOOKUP="data/raw/zones/taxi_zone_lookup.csv"
if [ -f "$ZONE_LOOKUP" ]; then
    echo "⏭️  Zone lookup table already exists, skipping..."
else
    echo "📊 Downloading taxi zone lookup table..."
    curl -o "$ZONE_LOOKUP" \
         "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    echo "✓ Saved to: $ZONE_LOOKUP"
fi

# Download taxi zones shapefile (ZIP)
ZONE_SHAPES="data/raw/zones/shapes/taxi_zones.zip"
if [ -f "$ZONE_SHAPES" ]; then
    echo "⏭️  Zone shapefile already exists, skipping..."
else
    echo "🗺️  Downloading taxi zones shapefile..."
    curl -o "$ZONE_SHAPES" \
         "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    echo "✓ Downloaded: $ZONE_SHAPES"
    
    # Unzip the shapefile
    echo "📦 Extracting taxi zones shapefile..."
    unzip -o "$ZONE_SHAPES" -d "data/raw/zones/shapes/"
    echo "✓ Extracted to: data/raw/zones/shapes/"
fi

echo ""
echo "========================================"
echo "Downloading NYC Yellow Taxi trip data"
echo "Date range: $START_DATE to $END_DATE"
echo "========================================"

# Counter for downloaded and skipped files
DOWNLOADED=0
SKIPPED=0
FAILED=0
NOT_AVAILABLE=0

# Function to check if URL is valid (returns 0 if valid, 1 if not)
check_url() {
    local url=$1
    
    # Use HEAD request to check if URL exists without downloading
    # -s: silent, -f: fail on HTTP errors, -I: HEAD request only, -L: follow redirects
    if curl -s -f -I -L "$url" >/dev/null 2>&1; then
        return 0  # URL is valid
    else
        return 1  # URL is invalid or not available
    fi
}

# Function to download a single month
download_month() {
    local year=$1
    local month=$2
    
    # Format month with leading zero
    MONTH_PADDED=$(printf "%02d" "$month")
    
    # Construct filename and paths
    FILENAME="yellow_tripdata_${year}-${MONTH_PADDED}.parquet"
    OUTPUT_DIR="$OUTPUT_BASE/$year/$MONTH_PADDED"
    OUTPUT_FILE="$OUTPUT_DIR/$FILENAME"
    SOURCE_URL="$BASE_URL/$FILENAME"
    
    # Check if file already exists
    if [ -f "$OUTPUT_FILE" ]; then
        echo "⏭️  $FILENAME already exists, skipping..."
        SKIPPED=$((SKIPPED + 1))
    else
        # First, check if URL is valid
        echo "🔍 Checking $FILENAME..."
        
        if ! check_url "$SOURCE_URL"; then
            echo "❌ URL not available: $FILENAME"
            NOT_AVAILABLE=$((NOT_AVAILABLE + 1))
            return
        fi
        
        echo "📥 Downloading $FILENAME..."
        mkdir -p "$OUTPUT_DIR"
        
        # Use -f flag to fail on HTTP errors, with || to handle 404s gracefully
        if curl -f -o "$OUTPUT_FILE" "$SOURCE_URL" 2>/dev/null; then
            # Verify the downloaded file is actually a parquet file, not an error response
            # Check for common error patterns (XML errors, HTML, or suspiciously small files)
            FILE_SIZE=$(stat -f%z "$OUTPUT_FILE" 2>/dev/null || stat -c%s "$OUTPUT_FILE" 2>/dev/null)
            
            if [ "$FILE_SIZE" -lt 1000 ]; then
                # File is suspiciously small, check if it's an error message
                if grep -q -E "(<Error>|<html>|Access Denied)" "$OUTPUT_FILE" 2>/dev/null; then
                    echo "⚠️  Downloaded file is an error response (Access Denied), not actual data"
                    echo "   File size: ${FILE_SIZE} bytes - removing..."
                    rm "$OUTPUT_FILE"
                    NOT_AVAILABLE=$((NOT_AVAILABLE + 1))
                else
                    echo "⚠️  Warning: File downloaded but only ${FILE_SIZE} bytes"
                    echo "✓ Saved to: $OUTPUT_FILE"
                    DOWNLOADED=$((DOWNLOADED + 1))
                fi
            else
                echo "✓ Saved to: $OUTPUT_FILE (${FILE_SIZE} bytes)"
                DOWNLOADED=$((DOWNLOADED + 1))
            fi
        else
            echo "⚠️  Failed to download $FILENAME"
            FAILED=$((FAILED + 1))
            # Remove the empty/failed file if it was created
            [ -f "$OUTPUT_FILE" ] && rm "$OUTPUT_FILE"
        fi
    fi
}

# Loop through all months in the date range
current_year=$START_YEAR
current_month=$START_MONTH_NUM

while true; do
    # Download the current month
    download_month "$current_year" "$current_month"
    
    # Check if we've reached the end date
    if [ "$current_year" -eq "$END_YEAR" ] && [ "$current_month" -eq "$END_MONTH_NUM" ]; then
        break
    fi
    
    # Increment to next month
    current_month=$((current_month + 1))
    
    # Roll over to next year if needed
    if [ "$current_month" -gt 12 ]; then
        current_month=1
        current_year=$((current_year + 1))
    fi
done

echo ""
echo "========================================"
echo "✅ Download process complete!"
echo "========================================"
echo "Files downloaded: $DOWNLOADED"
echo "Files skipped (already exist): $SKIPPED"
echo "Files not available: $NOT_AVAILABLE"
echo "Files failed: $FAILED"
echo ""
echo "Data locations:"
echo "  • Trip data: $OUTPUT_BASE/"
echo "  • Data dictionary: data/raw/dictionary/"
echo "  • Zone lookup: data/raw/zones/taxi_zone_lookup.csv"
echo "  • Zone shapefiles: data/raw/zones/shapes/"
echo ""
