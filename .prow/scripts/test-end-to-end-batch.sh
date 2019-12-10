#!/usr/bin/env bash

set -e
set -o pipefail

if ! cat /etc/*release | grep -q stretch; then
    echo ${BASH_SOURCE} only supports Debian stretch. 
    echo Please change your operating system to use this script.
    exit 1
fi

echo "
This script will run end-to-end tests for Feast Core and Batch Serving.

1. Install gcloud SDK
2. Install Redis as the job store for Feast Batch Serving.
4. Install Postgres for persisting Feast metadata.
5. Install Kafka and Zookeeper as the Source in Feast.
6. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from 
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
Installing Redis at localhost:6379
============================================================
"
apt-get -qq update
# Allow starting serving in this Maven Docker image. Default set to not allowed.
echo "exit 0" > /usr/sbin/policy-rc.d
apt-get -y install redis-server wget > /var/log/redis.install.log
redis-server --daemonize yes
redis-cli ping

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
Installing Zookeeper at localhost:2181
Installing Kafka at localhost:9092
============================================================
"
wget -qO- https://www-eu.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz | tar xz
mv kafka_2.12-2.3.0/ /tmp/kafka
nohup /tmp/kafka/bin/zookeeper-server-start.sh /tmp/kafka/config/zookeeper.properties &> /var/log/zookeeper.log 2>&1 &
sleep 5
tail -n10 /var/log/zookeeper.log
nohup /tmp/kafka/bin/kafka-server-start.sh /tmp/kafka/config/server.properties &> /var/log/kafka.log 2>&1 &
sleep 10
tail -n10 /var/log/kafka.log

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
    runner: DirectRunner
    options: {}
    metrics:
      enabled: false

  stream:
    type: kafka
    options:
      topic: feast-features
      bootstrapServers: localhost:9092
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

nohup java -jar core/target/feast-core-0.3.2-SNAPSHOT.jar \
  --spring.config.location=file:///tmp/core.application.yml \
  &> /var/log/feast-core.log &
sleep 30
tail -n10 /var/log/feast-core.log
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
  - name: "*"
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
  port: 6566
  enable-reflection: true
spring:
  main:
    web-environment: false
EOF

nohup java -jar serving/target/feast-serving-0.3.2-SNAPSHOT.jar \
  --spring.config.location=file:///tmp/serving.warehouse.application.yml \
  &> /var/log/feast-serving-warehouse.log &
sleep 15
tail -n100 /var/log/feast-serving-warehouse.log

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
pytest bq-batch-retrieval.py --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
TEST_EXIT_CODE=$?

cd ${ORIGINAL_DIR}
exit ${TEST_EXIT_CODE}

echo "
============================================================
Cleaning up
============================================================
"

bq rm -r -f kf-feast:$DATASET_NAME
