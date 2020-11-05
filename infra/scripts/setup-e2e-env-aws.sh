#!/bin/bash

make compile-protos-python

python -m pip install --upgrade pip setuptools wheel

python -m pip install -qr sdk/python/requirements-dev.txt
python -m pip install -qr tests/requirements.txt

# Using mvn -q to make it less verbose. This step happens after docker containers were
# succesfully built so it should be unlikely to fail.
echo "########## Building ingestion jar"
TIMEFORMAT='########## took %R seconds'
time mvn -q --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -DskipUTs=true clean package
