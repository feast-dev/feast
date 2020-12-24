#!/usr/bin/env bash

export DISABLE_SERVICE_FIXTURES=1
export MAVEN_OPTS="-Dmaven.repo.local=/tmp/.m2/repository -DdependencyLocationsEnabled=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=3 -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false"
export MAVEN_CACHE="gs://feast-templocation-kf-feast/.m2.2020-11-17.tar"

infra/scripts/download-maven-cache.sh --archive-uri ${MAVEN_CACHE} --output-dir /tmp
apt-get update && apt-get install -y redis-server postgresql libpq-dev

make build-java-no-tests REVISION=develop

python -m pip install --upgrade pip setuptools wheel
make install-python
python -m pip install -qr tests/requirements.txt

su -p postgres -c "PATH=$PATH HOME=/tmp pytest -v tests/e2e/ \
      --feast-version develop --env=gcloud --dataproc-cluster-name feast-e2e \
      --dataproc-project kf-feast --dataproc-region us-central1 \
      --staging-path gs://feast-templocation-kf-feast/ \
      --with-job-service \
      --redis-url 10.128.0.105:6379 --redis-cluster --kafka-brokers 10.128.0.103:9094 \
      --bq-project kf-feast"
