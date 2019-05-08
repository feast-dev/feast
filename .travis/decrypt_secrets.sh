#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Decrypt Google Cloud service account JSON key for BigQuery and GKE access
openssl aes-256-cbc \
-K $encrypted_deb1d355a9cd_key \
-iv $encrypted_deb1d355a9cd_iv \
-in ${SCRIPT_DIR}/service_account.json.enc \
-out ${SCRIPT_DIR}/service_account.json \
-d
