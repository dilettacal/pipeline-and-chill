# Demo Scripts Notes

## demo_replay_producer.py

### Known Issues

#### PyArrow Warnings on macOS
You may see warnings like:
```
IOError: sysctlbyname failed for 'hw.l1dcachesize'
```

**These are harmless** - PyArrow is trying to get CPU cache information but is blocked by macOS permissions. The script works perfectly despite these warnings.

### Implementation Notes

#### Reading Parquet Files with Sample Size
**Issue**: `pd.read_parquet()` doesn't support the `nrows` parameter (unlike `read_csv()`)

**Solution**: Read the entire file first, then use `.head(n)` to get sample:
```python
# ❌ This doesn't work:
df = pd.read_parquet(path, nrows=1000)

# ✅ Use this instead:
df = pd.read_parquet(path)
df = df.head(1000)
```

**Trade-off**: This reads the entire file into memory first, then samples. For very large files, this could be memory-intensive. However:
- Our May 2025 file is ~114 MB (manageable)
- Parquet files are columnar and compressed
- Alternative would require pyarrow API directly (more complex)

For production use with extremely large files, consider:
```python
import pyarrow.parquet as pq
table = pq.read_table(path, columns=['column1', 'column2'])  # Select columns
df = table.to_pandas()
```

### Prerequisites

The demo script checks for:
1. ✅ Kafka running at `localhost:19092`
2. ✅ Curated data exists at `data/curated/yellow/2025/05/trips_clean.parquet`
3. ⚠️ Kafka topic `trips.normalized` (creates if missing)

### Usage

```bash
# Interactive demo (recommended)
make demo-replay

# Direct execution
python scripts/demo/demo_replay_producer.py
```

### What It Does

1. **Prerequisites Check** - Validates environment
2. **Event Decomposition** - Shows 1 trip → 5 events
3. **Small Replay** - Publishes 1,000 trips (~5,000 events)
4. **Verification** - Consumes and displays sample events
5. **Statistics** - Shows topic metrics

### Expected Runtime

- Prerequisites check: ~2-3 seconds
- Event decomposition: ~1 second
- Small replay (1,000 trips): ~2-5 seconds
- Verification: ~2-3 seconds

**Total**: ~10-15 seconds (interactive pauses not included)

### Troubleshooting

#### "Connection refused" to Kafka
```bash
make start
# Wait 10-15 seconds for Kafka to be ready
```

#### "FileNotFoundError: Curated file not found"
```bash
cd packages/ingestion-monthly-loader
python -m fluxframe.cli --month 2025-05
```

#### Memory Issues on Small Machines
If reading 4.4M rows is too much, reduce sample size:
```python
# In demo_replay_producer.py, line ~341
published_count = run_small_replay(data_path, sample_size=100)  # Instead of 1000
```
