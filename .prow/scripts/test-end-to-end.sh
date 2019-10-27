#!/usr/bin/env bash

set -e
set -o pipefail

if ! cat /etc/*release | grep -q stretch; then
    echo ${BASH_SOURCE} only supports Debian stretch. 
    echo Please change your operating system to use this script.
    exit 1
fi

echo "
This script will run end-to-end tests for Feast Online Serving.

1. Install Redis as the store for Feast Online Serving.
2. Install Postgres for persisting Feast metadata.
3. Install Kafka and Zookeeper as the Source in Feast.
4. Install Python 3.7.4, Feast Python SDK and run end-to-end tests from 
   tests/e2e via pytest.
"

# Install Redis at localhost:6379
apt-get -qq update
apt-get -y install redis-server wget
redis-server --daemonize yes
redis-cli ping

# Install Postgres at localhost:5432 
# Initialize with database 'postgres', user 'postgres', password 'password'
apt-get -y install postgresql
service postgresql start
cat <<EOF > /tmp/update-postgres-role.sh
psql -c "ALTER USER postgres PASSWORD 'password';"
EOF
chmod +x /tmp/update-postgres-role.sh
su -s /bin/bash -c /tmp/update-postgres-role.sh postgres
export PGPASSWORD=password
pg_isready

# Install Zookeeper at localhost:2181
# Install Kafka at localhost:9092
wget -qO- https://www-eu.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz | tar xz
mv kafka_2.12-2.3.0/ /tmp/kafka
nohup /tmp/kafka/bin/zookeeper-server-start.sh -daemon /tmp/kafka/config/zookeeper.properties > /var/log/zooker.log 2>&1 &
sleep 5
nohup /tmp/kafka/bin/kafka-server-start.sh -daemon /tmp/kafka/config/server.properties > /var/log/kafka.log 2>&1 &
sleep 5

# Build jars for Feast
mvn --batch-mode --define skipTests=true clean package

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

nohup java -jar core/target/feast-core-0.3.0-SNAPSHOT.jar \
  --spring.config.location=file:///tmp/core.application.yml \
  &> /var/log/feast-core.log &
sleep 20
tail -n50 /var/log/feast-core.log

# Start Feast Online Serving in background
cat <<EOF > /tmp/serving.store.redis.yml
name: serving
type: REDIS
redis_config:
  host: localhost
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
tail -n50 /var/log/feast-serving-online.log

# Install Python 3.7 with Miniconda
wget https://repo.continuum.io/miniconda/Miniconda3-4.7.12-Linux-x86_64.sh
bash /tmp/miniconda.sh -b -p /root/miniconda -f
/root/miniconda/bin/conda init
source ~/.bashrc

# Default artifact location setting in Prow jobs
LOGS_ARTIFACT_PATH=/logs/artifacts

# Run end-to-end tests with pytest with the Python SDK
pip install sdk/feast
pip install -r tests/e2e/requirements.txt

ORIGINAL_DIR=$(pwd)

cd tests/e2e
set +e
pytest --junitxml=${LOGS_ARTIFACT_PATH}/python-sdk-test-report.xml
TEST_EXIT_CODE=$?

cd ${ORIGINAL_DIR}
exit ${TEST_EXIT_CODE}
