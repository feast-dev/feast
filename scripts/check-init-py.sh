#!/bin/bash
# Check for missing __init__.py files in Python packages

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Find Python package directories missing __init__.py
missing_init_files=()

while IFS= read -r -d '' dir; do
    # Skip .ipynb_checkpoints directories and other unwanted directories
    if [[ "${dir}" == *".ipynb_checkpoints"* ]] || [[ "${dir}" == *"__pycache__"* ]]; then
        continue
    fi

    if [[ ! -f "${dir}/__init__.py" ]] && [[ -n "$(find "${dir}" -maxdepth 1 -name "*.py" -print -quit)" ]]; then
        missing_init_files+=("${dir}")
    fi
done < <(find "${ROOT_DIR}/sdk/python/feast" -type d -print0)

if [[ ${#missing_init_files[@]} -gt 0 ]]; then
    echo "❌ Missing __init__.py files in:"
    printf "   %s\n" "${missing_init_files[@]}"
    echo ""
    echo "Run: touch ${missing_init_files[*]/%//__init__.py}"
    exit 1
fi

echo "✅ All Python packages have __init__.py files"
