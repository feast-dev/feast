#!/usr/bin/env bash

install_test_tools() {
  apt-get -qq update
  apt-get -y install wget netcat kafkacat build-essential
}

install_gcloud_sdk() {
  print_banner "Installing Google Cloud SDK"
  if [[ ! $(command -v gsutil) ]]; then
    CURRENT_DIR=$(dirname "$BASH_SOURCE")
    . "${CURRENT_DIR}"/install-google-cloud-sdk.sh
  fi

  export GOOGLE_APPLICATION_CREDENTIALS
  gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
}

install_and_start_local_redis() {
  print_banner "Installing and tarting Redis at localhost:6379"
  # Allow starting serving in this Maven Docker image. Default set to not allowed.
  echo "exit 0" >/usr/sbin/policy-rc.d
  apt-get -y install redis-server >/var/log/redis.install.log
  redis-server --daemonize yes
  redis-cli ping
}

install_and_start_local_postgres() {
  print_banner "Installing and starting Postgres at localhost:5432"
  apt-get -y install postgresql >/var/log/postgresql.install.log
  service postgresql start
  # Initialize with database: 'postgres', user: 'postgres', password: 'password'
  cat <<EOF >/tmp/update-postgres-role.sh
psql -c "ALTER USER postgres PASSWORD 'password';"
EOF
  chmod +x /tmp/update-postgres-role.sh
  su -s /bin/bash -c /tmp/update-postgres-role.sh postgres
  export PGPASSWORD=password
  pg_isready
}

install_and_start_local_zookeeper_and_kafka() {
  print_banner "Installing and starting Zookeeper at localhost:2181 and Kafka at localhost:9092"
  wget -qO- https://www-eu.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz | tar xz
  mv kafka_2.12-2.3.0/ /tmp/kafka
  nohup /tmp/kafka/bin/zookeeper-server-start.sh /tmp/kafka/config/zookeeper.properties &>/var/log/zookeeper.log 2>&1 &
  sleep 5
  tail -n10 /var/log/zookeeper.log
  nohup /tmp/kafka/bin/kafka-server-start.sh /tmp/kafka/config/server.properties &>/var/log/kafka.log 2>&1 &
  sleep 20
  tail -n10 /var/log/kafka.log
  kafkacat -b localhost:9092 -L
}

build_feast_core_and_serving() {
  print_banner "Building Feast Core and Feast Serving"
  infra/scripts/download-maven-cache.sh \
    --archive-uri gs://feast-templocation-kf-feast/.m2.2019-10-24.tar \
    --output-dir /root/

  # Build jars for Feast
  mvn --quiet --batch-mode --define skipTests=true clean package

  ls -lh core/target/*jar
  ls -lh serving/target/*jar
}

start_feast_core() {
  print_banner "Starting Feast Core"

  if [ -n "$1" ]; then
    echo "Custom Spring application.yml location provided: $1"
    export CONFIG_ARG="--spring.config.location=file://$1"
  fi

  nohup java -jar core/target/feast-core-$FEAST_BUILD_VERSION.jar $CONFIG_ARG &> /var/log/feast-core.log &
  ./wait-for-it.sh localhost:6565 --timeout=90

  tail -n10 /var/log/feast-core.log
  nc -w2 localhost 6565 </dev/null
}

start_feast_serving() {
  print_banner "Starting Feast Online Serving"

  if [ -n "$1" ]; then
    echo "Custom Spring application.yml location provided: $1"
    export CONFIG_ARG="--spring.config.location=file://$1"
  fi

  nohup java -jar serving/target/feast-serving-$FEAST_BUILD_VERSION.jar $CONFIG_ARG &>/var/log/feast-serving-online.log &
  ./wait-for-it.sh localhost:6566 --timeout=60

  tail -n100 /var/log/feast-serving-online.log
  nc -w2 localhost 6566 </dev/null
}

install_python_with_miniconda_and_feast_sdk() {
  print_banner "Installing Python 3.7 with Miniconda and Feast SDK"
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
}

print_banner() {
  echo "
============================================================
$1
============================================================
"
}
