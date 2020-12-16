#!/usr/bin/env bash

VERSION=$1
DESTINATION=$2
PACKAGES=${PACKAGES:-"great-expectations==0.13.2"}

tmp_dir=$(mktemp -d)

pip3 install -t ${tmp_dir}/libs $PACKAGES

cd $tmp_dir
tar -czf libs-$VERSION.tar.gz libs/
if [[ $DESTINATION == gs* ]]; then
  gsutil cp libs-$VERSION.tar.gz $GS_DESTINATION/libs-$VERSION.tar.gz
else
  mv libs-$VERSION.tar.gz $DESTINATION
fi