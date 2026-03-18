#!/bin/bash
# UV runner script for consistent environment handling

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Change to SDK directory for Python operations
cd "${ROOT_DIR}/sdk/python"

# Run uv with provided arguments
exec uv "$@"
