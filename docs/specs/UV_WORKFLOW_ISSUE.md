# UV Workflow Issue & Resolution

**Date**: 2026-01-14  
**Issue**: `uv sync --extra iceberg` fails when building pyarrow from source

## Problem

When running `uv sync --extra iceberg`, uv attempts to build `pyarrow==17.0.0` from source, which requires:
- Apache Arrow C++ libraries (libarrow)
- Arrow development headers
- CMake build toolchain

**Error**:
```
CMake Error: Could not find a package configuration file provided by "Arrow"
```

## Root Cause

The `pyarrow` package on PyPI provides pre-built wheels for many platforms, but uv may try to build from source if:
1. No compatible wheel exists for the platform
2. The package index doesn't have the wheel
3. uv's cache is inconsistent

## Solution Options

### Option 1: Install Arrow C++ Development Libraries (RECOMMENDED)

**For Ubuntu/Debian**:
```bash
# Add Apache Arrow APT repository
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo dpkg -i apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update

# Install Arrow C++ and development files
sudo apt install -y libarrow-dev libarrow-dataset-dev libarrow-flight-dev libarrow-python-dev

# Now run uv sync
uv sync --extra iceberg
```

**For macOS (Homebrew)**:
```bash
brew install apache-arrow

# Now run uv sync
uv sync --extra iceberg
```

**For WSL (Windows Subsystem for Linux)**:
```bash
# Use Ubuntu instructions above
```

### Option 2: Use Pre-built Wheels with uv

Force uv to use pre-built wheels only:

```bash
# Try with --no-build to prefer binary wheels
uv sync --extra iceberg --no-build

# Or specify platform explicitly
uv sync --extra iceberg --only-binary pyarrow
```

### Option 3: Use Existing Working Environment (FALLBACK)

Since the existing venv already has pyarrow built and working:

```bash
# Check current environment
source venv/bin/activate
python -c "import pyarrow; print(f'✅ pyarrow {pyarrow.__version__}')"

# Install additional iceberg deps in existing venv using uv
uv pip install --python venv/bin/python "pyiceberg[sql,duckdb]>=0.8.0" "duckdb>=1.0.0"
```

**Note**: This uses `uv pip` which is acceptable when targeting a specific Python environment.

## Current Workaround (Temporary)

Use the existing venv that already has all dependencies:

```bash
# Activate existing venv
cd /home/tommyk/projects/dataops/feast/sdk/python
source ../../venv/bin/activate

# Run tests using activated venv's pytest
pytest tests/integration/offline_store/test_universal_historical_retrieval.py \
  -v --maxfail=5 --tb=short
```

**Trade-off**: This bypasses uv's environment management but allows us to proceed with testing.

## Recommendation

**For Production/CI**: Install Arrow C++ libraries (Option 1)  
**For Local Development**: Use Option 2 (pre-built wheels) or Option 3 (existing venv)  
**For Immediate Progress**: Use current workaround with existing venv

## Next Steps

1. **Short-term**: Proceed with tests using existing venv
2. **Medium-term**: Install Arrow C++ dev libs for full uv workflow
3. **Long-term**: Document Arrow dependency requirements in README

## Updated Test Commands

**Using Existing Venv** (current approach):
```bash
cd /home/tommyk/projects/dataops/feast/sdk/python
source ../../venv/bin/activate
pytest tests/integration/offline_store/test_universal_historical_retrieval.py \
  -v --maxfail=5 --tb=short 2>&1 | tee iceberg_integration_tests.log
```

**Using UV** (after installing Arrow C++ or with --no-build):
```bash
cd /home/tommyk/projects/dataops/feast
uv sync --extra iceberg --no-build  # Try pre-built wheels first
uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py \
  -v --maxfail=5 --tb=short
```

---

**Status**: Documented uv workflow issue  
**Action**: Proceeding with existing venv for immediate progress  
**Future**: Install Arrow C++ libs for full uv native workflow
