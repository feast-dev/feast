#!/usr/bin/env sh

set -e

cd /app
python materialize.py
feast serve_transformations --port 8080
