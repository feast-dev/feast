#!/usr/bin/env bash

set -e
set -o pipefail

if ! cat /etc/*release | grep -q stretch; then
    echo ${BASH_SOURCE} only supports Debian stretch. 
    echo Please change your operating system to use this script.
    exit 1
fi

echo "
This script will run end-to-end tests for Feast Core and Online Serving.
1. Install Redis as the store for Feast Online Serving.
2. Install Postgres for persisting Feast metadata.
3. Install Kafka and Zookeeper as the Source in Feast.
4. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from 
   tests/e2e via pytest.
"

echo "
============================================================
Installing gcloud SDK
============================================================
"
if [[ ! $(command -v gsutil) ]]; then
  CURRENT_DIR=$(dirname "$BASH_SOURCE")
  . "${CURRENT_DIR}"/install_google_cloud_sdk.sh
fi

export GOOGLE_APPLICATION_CREDENTIALS=/etc/service-account/service-account.json
gcloud auth activate-service-account --key-file /etc/service-account/service-account.json


echo "
============================================================
Installing Postgres at localhost:5432
============================================================
"
apt-get -y install postgresql > /var/log/postgresql.install.log
service postgresql start
# Initialize with database: 'postgres', user: 'postgres', password: 'password'
cat <<EOF > /tmp/update-postgres-role.sh
psql -c "ALTER USER postgres PASSWORD 'password';"
EOF
chmod +x /tmp/update-postgres-role.sh
su -s /bin/bash -c /tmp/update-postgres-role.sh postgres
export PGPASSWORD=password
pg_isready

echo "
============================================================
Starting remote kafka and redis instances

============================================================
"
INSTANCE_NAME=feast_$(date +%s)
REMOTE_HOST=${INSTANCE_NAME}.us-central1.c.kf-feast.internal

gcloud compute instances create $INSTANCE_NAME \
  --source-instance-template feast-external-resources

sleep 10

echo "
============================================================
Building jars for Feast
============================================================
"

.prow/scripts/download-maven-cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar \
    --output-dir /root/

# Build jars for Feast
mvn --quiet --batch-mode --define skipTests=true clean package

echo "
============================================================
Starting Feast Core
============================================================
"
# Start Feast Core in background
cat <<EOF > /tmp/core.application.yml
grpc:
  port: 6565
  enable-reflection: true
feast:
  version: 0.3
  jobs:
    runner: DataflowRunner
    options:
      project: kf-feast
      tempLocation: gs://feast-templocation-kf-feast/staging-location
      region: us-central1
    metrics:
      enabled: false
  stream:
    type: kafka
    options:
      topic: feast-features
      bootstrapServers: $REMOTE_HOST:9092
      replicationFactor: 1
      partitions: 1
spring:
  jpa:
    properties.hibernate.format_sql: true
    hibernate.naming.physical-strategy=org.hibernate.boot.model.naming: PhysicalNamingStrategyStandardImpl
    hibernate.ddl-auto: update
  datasource:
    url: jdbc:postgresql://localhost:5432/postgres
    username: postgres
    password: password
management:
  metrics:
    export:
      simple:
        enabled: false
      statsd:
        enabled: false
EOF

nohup java -jar core/target/feast-core-0.3.0-SNAPSHOT.jar \
  --spring.config.location=file:///tmp/core.application.yml \
  &> /var/log/feast-core.log &
sleep 20
tail -n10 /var/log/feast-core.log

echo "
============================================================
Starting Feast Online Serving
============================================================
"
# Start Feast Online Serving in background
cat <<EOF > /tmp/serving.store.redis.yml
name: serving
type: REDIS
redis_config:
  host: $REMOTE_HOST
  port: 6379
subscriptions:
  - name: .*
    version: ">0"
EOF

cat <<EOF > /tmp/serving.online.application.yml
feast:
  version: 0.3
  core-host: localhost
  core-grpc-port: 6565
  tracing:
    enabled: false
  store:
    config-path: /tmp/serving.store.redis.yml
    redis-pool-max-size: 128
    redis-pool-max-idle: 16
  jobs:
    staging-location: gs://feast-templocation-kf-feast/staging-location
    store-type:
    store-options: {}
grpc:
  port: 6566
  enable-reflection: true
spring:
  main:
    web-environment: false
EOF

nohup java -jar serving/target/feast-serving-0.3.0-SNAPSHOT.jar \
  --spring.config.location=file:///tmp/serving.online.application.yml \
  &> /var/log/feast-serving-online.log &
sleep 15
tail -n10 /var/log/feast-serving-online.log

echo "
============================================================
Starting Feast Warehouse Serving
============================================================
"

DATASET_NAME=feast_$(date +%s)

bq --location=US --project_id=kf-feast mk \
  --dataset \
  --default_table_expiration 86400 \
  kf-feast:$DATASET_NAME

# Start Feast Online Serving in background
cat <<EOF > /tmp/serving.store.bigquery.yml
name: warehouse
type: BIGQUERY
bigquery_config:
  projectId: kf-feast
  datasetId: $DATASET_NAME
subscriptions:
  - name: .*
    version: ">0"
EOF

cat <<EOF > /tmp/serving.warehouse.application.yml
feast:
  version: 0.3
  core-host: localhost
  core-grpc-port: 6565
  tracing:
    enabled: false
  store:
    config-path: /tmp/serving.store.bigquery.yml
  jobs:
    staging-location: gs://feast-templocation-kf-feast/staging-location
    store-type: REDIS
    store-options:
      host: $REMOTE_HOST
      port: 6379
grpc:
  port: 6567
  enable-reflection: true
spring:
  main:
    web-environment: false
EOF

nohup java -jar serving/target/feast-serving-0.3.0-SNAPSHOT.jar \
  --spring.config.location=file:///tmp/serving.warehouse.application.yml \
  &> /var/log/feast-serving-warehouse.log &
sleep 15
tail -n10 /var/log/feast-serving-warehouse.log

echo "
============================================================
Installing Python 3.7 with Miniconda and Feast SDK
============================================================
"
# Install Python 3.7 with Miniconda
wget -q https://repo.continuum.io/miniconda/Miniconda3-4.7.12-Linux-x86_64.sh \
   -O /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p /root/miniconda -f
/root/miniconda/bin/conda init
source ~/.bashrc

# Install Feast Python SDK and test requirements
pip install -q sdk/python
pip install -qr tests/e2e/requirements.txt

echo "
============================================================
Running end-to-end tests with pytest at 'tests/e2e'
============================================================
"
# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

ORIGINAL_DIR=$(pwd)
cd tests/e2e

set +e
pytest --allow_dirty true -k 'TestLargeVolume' --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
TEST_EXIT_CODE=$?

cd ${ORIGINAL_DIR}

echo "
============================================================
Cleaning up
============================================================
"

bq rm -r -f kf-feast:$DATASET_NAME

exit ${TEST_EXIT_CODE}