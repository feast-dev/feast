#!/usr/bin/env bash

set -euo pipefail

MONGO_CONTAINER="${CHRONON_MONGO_CONTAINER:-chronon-mongo}"
MAIN_CONTAINER="${CHRONON_MAIN_CONTAINER:-chronon-main}"
NETWORK_NAME="${CHRONON_NETWORK:-chronon-net}"
SERVICE_PID_FILE="${CHRONON_SERVICE_PID_FILE:-/tmp/chronon-service.pid}"

if [[ -f "${SERVICE_PID_FILE}" ]]; then
  pid="$(cat "${SERVICE_PID_FILE}")"
  if kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" 2>/dev/null || true
  fi
  rm -f "${SERVICE_PID_FILE}"
fi

(docker rm -f "${MAIN_CONTAINER}" >/dev/null 2>&1) || true
(docker rm -f "${MONGO_CONTAINER}" >/dev/null 2>&1) || true
(docker network rm "${NETWORK_NAME}" >/dev/null 2>&1) || true
