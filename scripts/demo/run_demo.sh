#!/bin/bash
# Simple script to run the complete demo

set -e

echo "🧹 Cleaning up old processes..."
pkill -f "fluxframe.cli" || true
sleep 3

echo "🧹 Cleaning pipeline data..."
cd /Users/diletta/Developer/Projects/ai-data-flow-e2e
. .venv/bin/activate
python scripts/testing/cleanup_pipeline.py

echo "🔄 Resetting Kafka consumer group..."
docker exec fluxframe-redpanda rpk group delete fluxframe-assembler 2>&1 | grep -E "^(GROUP|fluxframe)" || echo "  (Group doesn't exist or already deleted)"

echo ""
echo "🚀 Starting Trip Assembler in background..."
cd /Users/diletta/Developer/Projects/ai-data-flow-e2e
. .venv/bin/activate
cd packages/stream-trip-assembler
nohup python -m fluxframe.cli --verbose > /tmp/assembler.log 2>&1 &
ASSEMBLER_PID=$!
echo "✅ Assembler started (PID: $ASSEMBLER_PID)"
echo "   View logs: tail -f /tmp/assembler.log"

echo ""
echo "⏳ Waiting for assembler to initialize..."
sleep 5

echo ""
echo "📊 Sending demo events..."
cd /Users/diletta/Developer/Projects/ai-data-flow-e2e

# Send events but skip the interactive parts
python scripts/demo/demo_trip_assembler.py <<EOF
EOF

echo ""
echo "⏳ Waiting for assembler to process events..."
echo "   (Check logs: tail -f /tmp/assembler.log)"
sleep 15

echo ""
echo "========================================================================"
echo "  📊 RESULTS"
echo "========================================================================"
echo ""

echo "📈 Total Trips in Database:"
docker exec fluxframe-postgres psql -U dev -d fluxframe -c "SELECT COUNT(*) as total_trips FROM stg.complete_trip;" -t

echo ""
echo "📋 Recent Trips:"
docker exec fluxframe-postgres psql -U dev -d fluxframe -c "
SELECT 
    LEFT(trip_key, 16) || '...' as trip_key,
    TO_CHAR(pickup_ts, 'YYYY-MM-DD HH24:MI') as pickup,
    fare_amount,
    passenger_count as passengers,
    source
FROM stg.complete_trip 
ORDER BY last_update_ts DESC 
LIMIT 5;" 

echo ""
echo "📊 Trips by Source:"
docker exec fluxframe-postgres psql -U dev -d fluxframe -c "
SELECT 
    source,
    COUNT(*) as trips,
    TO_CHAR(SUM(fare_amount), 'FM$999,990.00') as total_fare,
    TO_CHAR(AVG(fare_amount), 'FM$990.00') as avg_fare
FROM stg.complete_trip 
GROUP BY source 
ORDER BY trips DESC;" 

echo ""
echo "========================================================================"
echo "  ✅ Demo Complete!"
echo "========================================================================"
echo ""
echo "📂 Logs:"
echo "   Assembler: tail -f /tmp/assembler.log"
echo ""
echo "🛑 To stop assembler: kill $ASSEMBLER_PID"
echo ""

