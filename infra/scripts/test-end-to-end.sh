#!/usr/bin/env bash

apt-get update && apt-get install -y redis-server postgresql libpq-dev

make build-java-no-tests REVISION=develop
python -m pip install --upgrade pip setuptools wheel
make install-python
python -m pip install -qr tests/requirements.txt

su -p postgres -c "PATH=$PATH HOME=/tmp pytest -s -v -x tests/e2e/ --feast-version develop"