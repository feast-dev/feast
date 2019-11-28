#!/bin/sh

while ! nc -z core 6565; do
  echo "Waiting for Feast core grpc service..."
  sleep 5
done

while ! nc -z serving 6566; do
  echo "Waiting for Feast serving grpc service..."
  sleep 5
done

cd /usr/src/tests/e2e/
pytest