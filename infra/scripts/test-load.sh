#!/usr/bin/env bash

set -e

echo "
============================================================
Running Load Tests
============================================================
"

clean_up() {
  ARG=$?

  # Shut down docker-compose images
  cd "${PROJECT_ROOT_DIR}"/infra/docker-compose

  docker-compose \
    -f docker-compose.yml \
    -f docker-compose.online.yml down

  # Remove configuration file
  rm .env

  exit $ARG
}

CURRENT_SHA=$(git rev-parse HEAD)

if [ "$GITHUB_EVENT_NAME" == "pull_request" ]; then
    export CURRENT_SHA=$(cat "$GITHUB_EVENT_PATH" | jq -r .pull_request.head.sha)
fi

export PROJECT_ROOT_DIR=$(git rev-parse --show-toplevel)
export COMPOSE_INTERACTIVE_NO_CLI=1

# Wait for docker images to be available
"${PROJECT_ROOT_DIR}"/infra/scripts/wait-for-docker-images.sh "${CURRENT_SHA}"

# Clean up Docker Compose if failure
trap clean_up EXIT

# Create Docker Compose configuration file
cd "${PROJECT_ROOT_DIR}"/infra/docker-compose/
cp .env.sample .env

# Start Docker Compose containers
FEAST_VERSION=${CURRENT_SHA} docker-compose -f docker-compose.yml -f docker-compose.online.yml up -d

# Get Jupyter container IP address
export JUPYTER_DOCKER_CONTAINER_IP_ADDRESS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' feast_jupyter_1)

# Print Jupyter container information
docker inspect feast_jupyter_1
docker logs feast_jupyter_1

# Wait for Jupyter Notebook Container to come online
"${PROJECT_ROOT_DIR}"/infra/scripts/wait-for-it.sh ${JUPYTER_DOCKER_CONTAINER_IP_ADDRESS}:8888 --timeout=60

# Get Feast Core container IP address
export FEAST_CORE_CONTAINER_IP_ADDRESS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' feast_core_1)

# Wait for Feast Core to be ready
"${PROJECT_ROOT_DIR}"/infra/scripts/wait-for-it.sh ${FEAST_CORE_CONTAINER_IP_ADDRESS}:6565 --timeout=120

# Get Feast Online Serving container IP address
export FEAST_ONLINE_SERVING_CONTAINER_IP_ADDRESS=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' feast_online-serving_1)

# Wait for Feast Online Serving to be ready
"${PROJECT_ROOT_DIR}"/infra/scripts/wait-for-it.sh ${FEAST_ONLINE_SERVING_CONTAINER_IP_ADDRESS}:6566 --timeout=120

# Ingest data into Feast
pip install --user matplotlib feast pytz matplotlib --upgrade
python "${PROJECT_ROOT_DIR}"/tests/load/ingest.py "${FEAST_CORE_CONTAINER_IP_ADDRESS}":6565  "${FEAST_ONLINE_SERVING_CONTAINER_IP_ADDRESS}":6566

# Download load test tool and proxy
cd $(mktemp -d)
wget -c https://github.com/feast-dev/feast-load-test-proxy/releases/download/v0.1.1/feast-load-test-proxy_0.1.1_Linux_x86_64.tar.gz -O - | tar -xz
git clone https://github.com/giltene/wrk2.git
cd wrk2
make
cd ..
cp wrk2/wrk .

# Start load test server
LOAD_FEAST_SERVING_HOST=${FEAST_ONLINE_SERVING_CONTAINER_IP_ADDRESS} LOAD_FEAST_SERVING_PORT=6566 ./feast-load-test-proxy &
sleep 5

# Run load tests
./wrk -t2 -c10 -d30s -R20 --latency http://localhost:8080/echo
./wrk -t2 -c10 -d30s -R20 --latency http://localhost:8080/send?entity_count=10 > load_test_results_1fs_13f_10e_20rps
./wrk -t2 -c10 -d30s -R50 --latency http://localhost:8080/send?entity_count=10 > load_test_results_1fs_13f_10e_50rps
./wrk -t2 -c10 -d30s -R250 --latency http://localhost:8080/send?entity_count=10 > load_test_results_1fs_13f_10e_250rps
./wrk -t2 -c10 -d30s -R20 --latency http://localhost:8080/send?entity_count=50 > load_test_results_1fs_13f_50e_20rps
./wrk -t2 -c10 -d30s -R50 --latency http://localhost:8080/send?entity_count=50 > load_test_results_1fs_13f_50e_50rps
./wrk -t2 -c10 -d30s -R250 --latency http://localhost:8080/send?entity_count=50 > load_test_results_1fs_13f_50e_250rps

# Print load test results
cat $(ls -lah | grep load_test_results | awk '{print $9}' | tr '\n' ' ')

# Create hdr-plot of load tests
export PLOT_FILE_NAME="load_test_graph_${CURRENT_SHA}"_$(date "+%Y%m%d-%H%M%S").png
python $PROJECT_ROOT_DIR/tests/load/hdr_plot.py --output "$PLOT_FILE_NAME" --title "Load test: ${CURRENT_SHA}" $(ls -lah | grep load_test_results | awk '{print $9}' | tr '\n' ' ')

# Persist artifact
mkdir -p "${PROJECT_ROOT_DIR}"/load-test-output/
cp "${PLOT_FILE_NAME}" "${PROJECT_ROOT_DIR}"/load-test-output/
