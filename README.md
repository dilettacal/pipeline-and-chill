# Pipeline and Chill 🚀

A modern data pipeline with streaming capabilities, built for scale and simplicity.


## Testing

### Option 1: Run from package directory (isolated)
```bash
cd backend/chillflow-core
uv run pytest tests/ -v
```

### Option 2: Run from project root (monorepo)
```bash
# Install package in editable mode (required for monorepo)
uv add --editable backend/chillflow-core

# Run tests from root
uv run pytest backend/chillflow-core/tests/ -v
```

**Note:** The `--editable` flag is crucial for development! It installs the package in development mode, so changes to the code are immediately reflected without reinstalling.