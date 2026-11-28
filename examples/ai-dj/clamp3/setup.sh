#!/usr/bin/env bash
# Fetch the CLaMP 3 code (MIT) and weights, and create this folder's own env.
# CLaMP 3 is not a pip package and pins transformers 4.40, so it lives apart
# from the main demo env.
set -euo pipefail
cd "$(dirname "$0")"

COMMIT=9016d2b
if [ ! -d vendor/clamp3 ]; then
  git clone -q https://github.com/sanderwood/clamp3.git vendor/clamp3
  git -C vendor/clamp3 checkout -q "$COMMIT"
fi

WEIGHTS=weights_clamp3_saas_h_size_768_t_model_FacebookAI_xlm-roberta-base_t_length_128_a_size_768_a_layers_12_a_length_128_s_size_768_s_layers_12_p_size_64_p_length_512.pth
mkdir -p weights
if [ ! -f "weights/$WEIGHTS" ]; then
  curl -L -o "weights/$WEIGHTS" "https://huggingface.co/sander-wood/clamp3/resolve/main/$WEIGHTS"
fi

uv sync
