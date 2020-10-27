export DISABLE_SERVICE_FIXTURES=1

apt-get update && apt-get install -y redis-server postgresql libpq-dev

make build-java-no-tests REVISION=develop

python -m pip install --upgrade pip setuptools wheel
make install-python
python -m pip install -qr tests/requirements.txt

su -p postgres -c "PATH=$PATH HOME=/tmp pytest tests/e2e/ \
          --feast-version develop --env=gcloud --dataproc-cluster-name feast-e2e \
          --dataproc-project kf-feast --dataproc-region us-central1 \
          --redis-url 10.128.0.105:6379 --redis-cluster --kafka-brokers 10.128.0.103:9094"