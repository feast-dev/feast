#!/usr/bin/env bash
# One-time setup for the Feast AI DJ. Safe to run again: every step skips
# work that is already done.
#
#   ./setup.sh               # 12 songs: quick, good for trying it out
#   TRACKS=1000 ./setup.sh   # the 1,000-song catalog from the blog post
#
# Needs uv, Ollama, Navidrome, ffmpeg, git and curl. The first run downloads
# the LLM (2.2 GB) and the CLaMP 3 and MERT models (~2 GB). The songs are
# about 1 MB each. With 1,000 songs, embedding takes another 10-20 minutes.
set -euo pipefail
cd "$(dirname "$0")"

MODEL="${OLLAMA_MODEL:-granite4.2:3b}"
TRACKS="${TRACKS:-12}"
step() { printf '\n==> %s\n' "$*"; }

step "Checking tools"
missing=""
for tool in uv ollama navidrome ffmpeg git curl; do
  command -v "$tool" >/dev/null || missing="$missing $tool"
done
if [ -n "$missing" ]; then
  echo "Missing:$missing"
  echo "On macOS you can get them with Homebrew: brew install$missing"
  exit 1
fi
echo "uv, ollama, navidrome, ffmpeg, git and curl found"

step "Python env"
uv sync -q

step "LLM ($MODEL on Ollama)"
if ! curl -s -o /dev/null http://127.0.0.1:11434/api/tags; then
  echo "Ollama is not running. Open the Ollama app (or run 'ollama serve'), then run ./setup.sh again."
  exit 1
fi
ollama list | grep -q "^$MODEL " || ollama pull "$MODEL"
echo "$MODEL is ready"

step "Music: $TRACKS FMA small tracks, spread over 8 genres"
uv run python scripts/prepare_catalog.py --tracks "$TRACKS"

step "CLaMP 3 music vectors (own env, it pins transformers 4.40)"
./clamp3/setup.sh
(cd clamp3 && .venv/bin/python embed_catalog.py)

step "Sound vectors and Feast offline data"
uv run python scripts/build_features.py

step "Feast: register the features and load them into the online store"
(
  cd feature_repo
  uv run feast apply
  uv run feast materialize-incremental "$(date -u +%Y-%m-%dT%H:%M:%S)"
)

step "Navidrome config and local login"
uv run python scripts/setup_navidrome.py

printf '\nSetup done. Start everything with ./start.sh\n'
