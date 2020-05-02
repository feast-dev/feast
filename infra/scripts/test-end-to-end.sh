#!/usr/bin/env bash

set -e
set -o pipefail

test -z ${GOOGLE_APPLICATION_CREDENTIALS} && GOOGLE_APPLICATION_CREDENTIALS="/etc/service-account/service-account.json"
test -z ${SKIP_BUILD_JARS} && SKIP_BUILD_JARS="false"
test -z ${GOOGLE_CLOUD_PROJECT} && GOOGLE_CLOUD_PROJECT="kf-feast"
test -z ${TEMP_BUCKET} && TEMP_BUCKET="feast-templocation-kf-feast"
test -z ${JOBS_STAGING_LOCATION} && JOBS_STAGING_LOCATION="gs://${TEMP_BUCKET}/staging-location"

# Get the current build version using maven (and pom.xml)
FEAST_BUILD_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo Building version: $FEAST_BUILD_VERSION

echo "
This script will run end-to-end tests for Feast Core and Online Serving.

1. Install Redis as the store for Feast Online Serving.
2. Install Postgres for persisting Feast metadata.
3. Install Kafka and Zookeeper as the Source in Feast.
4. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from
   tests/e2e via pytest.
"

apt-get -qq update
apt-get -y install wget netcat kafkacat build-essential

echo "
============================================================
Installing Redis at localhost:6379
============================================================
"
# Allow starting serving in this Maven Docker image. Default set to not allowed.
echo "exit 0" > /usr/sbin/policy-rc.d
apt-get -y install redis-server > /var/log/redis.install.log
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
sleep 20
tail -n10 /var/log/kafka.log
kafkacat -b localhost:9092 -L

if [[ ${SKIP_BUILD_JARS} != "true" ]]; then
echo "
============================================================
Building jars for Feast
============================================================
"

infra/scripts/download-maven-cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar \
    --output-dir /root/

# Build jars for Feast
mvn --quiet --batch-mode --define skipTests=true clean package

ls -lh core/target/*jar
ls -lh serving/target/*jar
else
  echo "[DEBUG] Skipping building jars"
fi

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
  jobs:
    polling_interval_milliseconds: 30000
    job_update_timeout_seconds: 240

    active_runner: direct

    runners:
      - name: direct
        type: DirectRunner
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
    properties.hibernate:
      format_sql: true
      event:
        merge:
          entity_copy_observer: allow
    hibernate.naming.physical-strategy=org.hibernate.boot.model.naming: PhysicalNamingStrategyStandardImpl
    hibernate.ddl-auto: update
  datasource:
    url: jdbc:postgresql://localhost:5432/postgres
    username: postgres
    password: password

EOF

nohup java -jar core/target/feast-core-$FEAST_BUILD_VERSION.jar \
  --spring.config.location=file:///tmp/core.application.yml \
  &> /var/log/feast-core.log &
sleep 35
tail -n10 /var/log/feast-core.log
nc -w2 localhost 6565 < /dev/null

echo "
============================================================
Starting Feast Online Serving
============================================================
"

cat <<EOF > /tmp/serving.online.application.yml
feast:
  core-host: localhost
  core-grpc-port: 6565

  active_store: serving

  # List of store configurations
  stores:
    - name: serving
      type: REDIS # Type of the store. REDIS, BIGQUERY are available options
      config:
        host: localhost
        port: 6379
      subscriptions:
        - name: "*"
          project: "*"
          version: "*"

  tracing:
    enabled: false

grpc:
  port: 6566
  enable-reflection: true

server:
  port: 8081

EOF

nohup java -jar serving/target/feast-serving-${FEAST_BUILD_VERSION}.jar \
  --spring.config.location=file:///tmp/serving.online.application.yml \
  &> /var/log/feast-serving-online.log &
sleep 15
tail -n100 /var/log/feast-serving-online.log
nc -w2 localhost 6566 < /dev/null

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
make compile-protos-python
pip install -qe sdk/python
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
pytest basic-ingest-redis-serving.py --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
TEST_EXIT_CODE=$?

if [[ ${TEST_EXIT_CODE} != 0 ]]; then
  echo "[DEBUG] Printing logs"
  ls -ltrh /var/log/feast*
  cat /var/log/feast-serving-online.log /var/log/feast-core.log

  echo "[DEBUG] Printing Python packages list"
  pip list
fi

cd ${ORIGINAL_DIR}
exit ${TEST_EXIT_CODE}
