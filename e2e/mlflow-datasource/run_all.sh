#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== MLflow DataSource E2E Test Suite ==="
if command -v oc >/dev/null 2>&1; then
  echo "Cluster: $(oc whoami --show-server 2>/dev/null || echo n/a)"
fi
echo "MLflow:  ${MLFLOW_TRACKING_URI:-not set}"

echo "[0/5] Seeding MLflow..."
python3 seed_mlflow.py

echo "[1/5] Happy path..."
python3 test_happy_path.py

echo "[2/5] Error handling..."
python3 test_error_handling.py

echo "[3/5] Auth..."
python3 test_auth.py

echo "[4/5] CLI..."
python3 test_cli.py

echo "[5/5] Backwards compatibility..."
python3 test_backwards_compat.py

echo "=== ALL TESTS PASSED ==="
