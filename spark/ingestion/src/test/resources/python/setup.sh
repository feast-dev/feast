#!/usr/bin/env bash

CURRENT_PATH=$PWD
DESTINATION=${DESTINATION:-$CURRENT_PATH}

# 1. Create libraries (dependencies) package
tmp_dir=$(mktemp -d)
pip3 install -t ${tmp_dir}/libs great-expectations pyarrow==2.0.0
cd $tmp_dir && tar -czf libs.tar.gz . && mv libs.tar.gz $DESTINATION/libs.tar.gz

# 2. Pickle python udf
cd $CURRENT_PATH
pip3 install great-expectations pyspark==3.0.1
python3 udf.py $DESTINATION/udf.pickle