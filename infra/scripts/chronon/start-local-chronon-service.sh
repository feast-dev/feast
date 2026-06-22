#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHRONON_DIR="${CHRONON_REPO:-${ROOT_DIR}/chronon}"

MONGO_CONTAINER="${CHRONON_MONGO_CONTAINER:-chronon-mongo}"
MAIN_CONTAINER="${CHRONON_MAIN_CONTAINER:-chronon-main}"
NETWORK_NAME="${CHRONON_NETWORK:-chronon-net}"
SERVICE_PORT="${CHRONON_SERVICE_PORT:-9000}"
SERVICE_HOST="${CHRONON_SERVICE_HOST:-127.0.0.1}"
SERVICE_PID_FILE="${CHRONON_SERVICE_PID_FILE:-/tmp/chronon-service.pid}"
SERVICE_LOG_FILE="${CHRONON_SERVICE_LOG_FILE:-/tmp/chronon-service.log}"
JAVA_BIN="${JAVA_BIN:-java}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CHRONON_PREFLIGHT_ONLY="${CHRONON_PREFLIGHT_ONLY:-0}"
MONGO_IMPL_JAR="${CHRONON_DIR}/quickstart/mongo-online-impl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar"
SERVICE_JAR="${CHRONON_SERVICE_JAR:-}"

find_service_jar() {
  if [[ -n "${SERVICE_JAR}" ]]; then
    echo "${SERVICE_JAR}"
    return
  fi

  find "${CHRONON_DIR}/service/target/scala-2.12" \
    -maxdepth 1 \
    -type f \
    -name "service-*.jar" \
    ! -name "*-sources.jar" \
    ! -name "*-javadoc.jar" \
    2>/dev/null | sort | tail -n 1
}

print_setup_help() {
  cat >&2 <<EOF

Set CHRONON_REPO to a local Chronon checkout with built quickstart and service jars.
Example:
  git clone https://github.com/airbnb/chronon.git /tmp/chronon
  cd /tmp/chronon/quickstart/mongo-online-impl && sbt assembly
  cd /tmp/chronon && sbt "project service" assembly
  CHRONON_REPO=/tmp/chronon infra/scripts/chronon/start-local-chronon-service.sh
EOF
}

SERVICE_JAR="$(find_service_jar)"

if [[ ! -d "${CHRONON_DIR}" ]]; then
  echo "Chronon repo not found at ${CHRONON_DIR}. Set CHRONON_REPO to a local checkout." >&2
  print_setup_help
  exit 1
fi

if [[ ! -f "${MONGO_IMPL_JAR}" ]]; then
  echo "Missing quickstart Mongo implementation jar at ${MONGO_IMPL_JAR}." >&2
  print_setup_help
  exit 1
fi

if [[ ! -f "${SERVICE_JAR}" ]]; then
  echo "Missing Chronon service jar at ${SERVICE_JAR}." >&2
  print_setup_help
  exit 1
fi

if [[ "${CHRONON_PREFLIGHT_ONLY}" == "1" ]]; then
  echo "Chronon local service preflight passed for CHRONON_REPO=${CHRONON_DIR}"
  exit 0
fi

cleanup_stale_service() {
  if [[ -f "${SERVICE_PID_FILE}" ]]; then
    local pid
    pid="$(cat "${SERVICE_PID_FILE}")"
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" 2>/dev/null || true
    fi
    rm -f "${SERVICE_PID_FILE}"
  fi
}

wait_for_mongo() {
  local attempts=60
  until docker exec "${MONGO_CONTAINER}" mongosh --quiet --eval 'db.runCommand({ ping: 1 }).ok' >/dev/null 2>&1; do
    attempts=$((attempts - 1))
    if [[ "${attempts}" -le 0 ]]; then
      echo "Mongo did not become ready in time." >&2
      exit 1
    fi
    sleep 2
  done
}

wait_for_data_load() {
  local attempts=90
  until docker logs "${MAIN_CONTAINER}" 2>&1 | grep -q "Spark session available as 'spark'"; do
    attempts=$((attempts - 1))
    if [[ "${attempts}" -le 0 ]]; then
      echo "Chronon quickstart data loader did not initialize Spark in time." >&2
      exit 1
    fi
    sleep 2
  done
}

wait_for_http() {
  local url="$1"
  local attempts=60
  until curl --fail --silent "${url}" >/dev/null; do
    if [[ -f "${SERVICE_PID_FILE}" ]] && ! kill -0 "$(cat "${SERVICE_PID_FILE}")" >/dev/null 2>&1; then
      echo "Chronon service exited before becoming ready. Log: ${SERVICE_LOG_FILE}" >&2
      tail -100 "${SERVICE_LOG_FILE}" >&2 || true
      exit 1
    fi
    attempts=$((attempts - 1))
    if [[ "${attempts}" -le 0 ]]; then
      echo "Chronon service did not become ready at ${url}." >&2
      tail -100 "${SERVICE_LOG_FILE}" >&2 || true
      exit 1
    fi
    sleep 2
  done
}

run_quickstart_online_prep() {
  docker exec "${MAIN_CONTAINER}" bash -lc '
    set -euo pipefail
    cd /srv/chronon
    run.py --conf production/group_bys/quickstart/purchases.v1 --mode upload --ds 2023-12-01
    run.py --conf production/group_bys/quickstart/returns.v1 --mode upload --ds 2023-12-01
    /opt/spark/bin/spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_purchases_v1_upload mongodb://admin:admin@'"${MONGO_CONTAINER}"':27017/?authSource=admin  # pragma: allowlist secret
    /opt/spark/bin/spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_returns_v1_upload mongodb://admin:admin@'"${MONGO_CONTAINER}"':27017/?authSource=admin  # pragma: allowlist secret
    run.py --mode metadata-upload --conf production/joins/quickstart/training_set.v2 --ds 2023-12-01
    run.py --mode fetch --type join --name quickstart/training_set.v2 -k "{\"user_id\":\"5\"}"
  '
}

cleanup_stale_service
(docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1) || docker network create "${NETWORK_NAME}" >/dev/null
(docker rm -f "${MONGO_CONTAINER}" >/dev/null 2>&1) || true
(docker rm -f "${MAIN_CONTAINER}" >/dev/null 2>&1) || true

docker run -d \
  --name "${MONGO_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=admin \
  mongo:latest >/dev/null

wait_for_mongo

docker run -d \
  --name "${MAIN_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  -p 4040:4040 \
  -e USER=root \
  -e SPARK_SUBMIT_PATH=/opt/spark/bin/spark-submit \
  -e PYTHONPATH=/srv/chronon \
  -e SPARK_VERSION=3.1.1 \
  -e JOB_MODE='local[*]' \
  -e PARALLELISM=2 \
  -e EXECUTOR_MEMORY=2G \
  -e EXECUTOR_CORES=4 \
  -e DRIVER_MEMORY=1G \
  -e CHRONON_LOG_TABLE=default.chronon_log_table \
  -e CHRONON_ONLINE_CLASS=ai.chronon.quickstart.online.ChrononMongoOnlineImpl \
  -e "CHRONON_ONLINE_ARGS=-Zuser=admin -Zpassword=admin -Zhost=${MONGO_CONTAINER} -Zport=27017 -Zdatabase=admin" \
  -v "${CHRONON_DIR}/quickstart/mongo-online-impl:/srv/onlineImpl" \
  ezvz/chronon \
  bash -lc '/opt/spark/bin/spark-shell -i scripts/data-loader.scala && tail -f /dev/null' >/dev/null

wait_for_data_load
run_quickstart_online_prep

"${PYTHON_BIN}" "${ROOT_DIR}/infra/scripts/chronon/chronon_service_launcher.py" \
  --chronon-dir "${CHRONON_DIR}" \
  --java-bin "${JAVA_BIN}" \
  --service-jar "${SERVICE_JAR}" \
  --service-port "${SERVICE_PORT}" \
  --log-file "${SERVICE_LOG_FILE}" \
  --pid-file "${SERVICE_PID_FILE}"
wait_for_http "http://${SERVICE_HOST}:${SERVICE_PORT}/ping"

echo "CHRONON_SERVICE_URL=http://${SERVICE_HOST}:${SERVICE_PORT}"
