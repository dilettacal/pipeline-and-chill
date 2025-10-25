#!/bin/bash
# Download NYC Taxi Reference Data
# - Data dictionary (PDF)
# - Taxi zone lookup table (CSV)
# - Taxi zone shapefiles (SHP)

set -e  # Exit on error

echo "========================================"
echo "Downloading NYC Taxi Reference Data"
echo "========================================"

# Create directories
mkdir -p "data/raw/dictionary"
mkdir -p "data/raw/zones"
mkdir -p "data/raw/zones/shapes"

# Counters
DOWNLOADED=0
SKIPPED=0
FAILED=0

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

# Function to download file with URL validation and skip check
download_file() {
    local url=$1
    local output_file=$2
    local description=$3

    echo ""
    echo "$description"

    # Check if file already exists
    if [ -f "$output_file" ]; then
        echo "⏭️  File already exists, skipping..."
        SKIPPED=$((SKIPPED + 1))
        return
    fi

    # Check if URL is valid
    echo "🔍 Checking URL..."
    if ! check_url "$url"; then
        echo "❌ URL not available: $url"
        FAILED=$((FAILED + 1))
        return
    fi

    # Download the file
    echo "📥 Downloading..."
    if curl -f -L -o "$output_file" "$url" 2>/dev/null; then
        echo "✓ Saved to: $output_file"
        DOWNLOADED=$((DOWNLOADED + 1))
    else
        echo "⚠️  Failed to download"
        FAILED=$((FAILED + 1))
        [ -f "$output_file" ] && rm "$output_file"
    fi
}

# Download data dictionary (PDF)
download_file \
    "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf" \
    "data/raw/dictionary/data_dictionary_trip_records_yellow.pdf" \
    "📄 Data dictionary (PDF)"

# Download taxi zone lookup table (CSV)
download_file \
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" \
    "data/raw/zones/taxi_zone_lookup.csv" \
    "📊 Taxi zone lookup table (CSV)"

# Download taxi zones shapefile (ZIP)
download_file \
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip" \
    "data/raw/zones/shapes/taxi_zones.zip" \
    "🗺️  Taxi zones shapefile (ZIP)"

# Unzip the shapefile if it was just downloaded or exists
ZONE_SHAPES="data/raw/zones/shapes/taxi_zones.zip"
if [ -f "$ZONE_SHAPES" ]; then
    # Check if shapefile has already been extracted
    if [ -f "data/raw/zones/shapes/taxi_zones.shp" ]; then
        echo ""
        echo "📦 Shapefile already extracted, skipping..."
    else
        echo ""
        echo "📦 Extracting taxi zones shapefile..."
        unzip -o "$ZONE_SHAPES" -d "data/raw/zones/shapes/"
        echo "✓ Extracted to: data/raw/zones/shapes/"
    fi
fi

echo ""
echo "========================================"
echo "✅ Reference data download complete!"
echo "========================================"
echo "Files downloaded: $DOWNLOADED"
echo "Files skipped (already exist): $SKIPPED"
echo "Files failed: $FAILED"
echo ""
echo "Data locations:"
echo "  • Data dictionary: data/raw/dictionary/data_dictionary_trip_records_yellow.pdf"
echo "  • Zone lookup: data/raw/zones/taxi_zone_lookup.csv"
echo "  • Zone shapefiles: data/raw/zones/shapes/"
echo ""
