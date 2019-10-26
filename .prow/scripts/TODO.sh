#!/usr/bin/env bash

set -o pipefail
set -e

apt-get -qq update

apt-get -y install redis-server wget
redis-server --daemonize yes
redis-cli ping

apt-get -y install postgresql
service postgresql start

cat <<EOF > /tmp/update-postgres-role.sh
psql -c "ALTER USER postgres PASSWORD 'password';"
EOF
chmod +x /tmp/update-postgres-role.sh
su -s /bin/bash -c /tmp/update-postgres-role.sh postgres
export PGPASSWORD=password
pg_isready

wget -qO- https://www-eu.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz | tar xz
cd kafka_2.12-2.3.0/
nohup bin/zookeeper-server-start.sh -daemon config/zookeeper.properties > /var/log/zooker.log 2>&1 &
sleep 5
nohup bin/kafka-server-start.sh -daemon config/server.properties > /var/log/kafka.log 2>&1 &
sleep 5

cd ..



mvn --batch-mode --define skipTests=true clean package
java -jar core/target/feast-core-0.3.0-SNAPSHOT.jar \
  --spring.config.location=file:///tmp/core.application.yml

java -jar serving/target/feast-serving-0.3.0-SNAPSHOT.jar \
  --spring.config.location=file:///tmp/serving.online.application.yml


wget https://repo.continuum.io/miniconda/Miniconda3-4.7.12-Linux-x86_64.sh
