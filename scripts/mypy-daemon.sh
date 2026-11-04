#!/bin/bash
# MyPy daemon for sub-second type checking

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MYPY_CACHE_DIR="${ROOT_DIR}/sdk/python/.mypy_cache"
PID_FILE="$MYPY_CACHE_DIR/dmypy.pid"

case "${1:-}" in
  start)
    echo "🚀 Starting MyPy daemon..."
    cd ${ROOT_DIR}/sdk/python
    uv run dmypy start -- --config-file=pyproject.toml
    echo "✅ MyPy daemon started"
    ;;
  check)
    echo "🔍 Running MyPy daemon check..."
    cd ${ROOT_DIR}/sdk/python
    time uv run dmypy check feast tests
    ;;
  stop)
    echo "🛑 Stopping MyPy daemon..."
    cd ${ROOT_DIR}/sdk/python
    uv run dmypy stop
    echo "✅ MyPy daemon stopped"
    ;;
  restart)
    echo "🔄 Restarting MyPy daemon..."
    $0 stop
    $0 start
    ;;
  status)
    echo "📊 MyPy daemon status:"
    cd ${ROOT_DIR}/sdk/python
    if uv run dmypy status; then
      echo "✅ MyPy daemon is running"
    else
      echo "❌ MyPy daemon is not running"
    fi
    ;;
  *)
    echo "Usage: $0 {start|check|stop|restart|status}"
    echo ""
    echo "Commands:"
    echo "  start   - Start the MyPy daemon"
    echo "  check   - Run type checking with the daemon"
    echo "  stop    - Stop the MyPy daemon"
    echo "  restart - Restart the daemon"
    echo "  status  - Check daemon status"
    exit 1
    ;;
esac
