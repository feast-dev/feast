#!/usr/bin/env sh

set -e

# feast root directory is expected to be mounted (eg, by docker compose)
cd /mnt/feast
pip install -e '.[redis]'

cd /app
python materialize.py
feast serve_transformations --port 8080