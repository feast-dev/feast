#!/usr/bin/env bash
#
# One-command setup and demo for the Feast-powered AI agent example.
#
# Usage:
#   cd examples/agent_feature_store
#   ./run_demo.sh              # demo mode (no API key needed)
#   OPENAI_API_KEY=sk-... ./run_demo.sh   # live LLM tool-calling
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-$(command -v python3 || command -v python || true)}"
if [[ -z "$PYTHON" ]]; then
    echo "ERROR: python3 or python not found on PATH."
    exit 1
fi
PIP="${PIP:-$(command -v pip3 || command -v pip || true)}"
if [[ -z "$PIP" ]]; then
    echo "ERROR: pip3 or pip not found on PATH."
    exit 1
fi

SERVER_PORT=6566
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        echo ""
        echo "Stopping Feast server (pid $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── 1. Install dependencies ─────────────────────────────────────────────────
echo "==> Step 1/4: Installing dependencies..."
$PIP install -q "feast[mcp,milvus]"

# ── 2. Generate data and apply registry ──────────────────────────────────────
echo ""
echo "==> Step 2/4: Generating sample data and applying Feast registry..."
$PYTHON setup_data.py

# ── 3. Start the Feast MCP server in the background ─────────────────────────
echo ""
echo "==> Step 3/4: Starting Feast MCP feature server on port $SERVER_PORT..."
cd feature_repo
feast serve --host 0.0.0.0 --port "$SERVER_PORT" --workers 1 &
SERVER_PID=$!
cd "$SCRIPT_DIR"

echo "    Waiting for server to become healthy..."
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${SERVER_PORT}/health" > /dev/null 2>&1; then
        echo "    Server is ready."
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Server process exited unexpectedly."
        exit 1
    fi
    sleep 1
done

if ! curl -sf "http://localhost:${SERVER_PORT}/health" > /dev/null 2>&1; then
    echo "ERROR: Server did not become healthy within 30 seconds."
    exit 1
fi

# ── 4. Run the agent ────────────────────────────────────────────────────────
echo ""
echo "==> Step 4/4: Running the agent..."
echo ""
$PYTHON agent.py

echo ""
echo "Demo complete."
