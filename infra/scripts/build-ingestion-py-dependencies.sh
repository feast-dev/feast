#!/usr/bin/env bash

VERSION=$1
DESTINATION=$2
PACKAGES=${PACKAGES:-"great-expectations==0.13.2"}

tmp_dir=$(mktemp -d)

pip3 install -t ${tmp_dir} $PACKAGES

cd $tmp_dir
zip -q -r libs-$VERSION.zip .
if [[ $DESTINATION == gs* ]]; then
  gsutil cp libs-$VERSION.zip $GS_DESTINATION/libs-$VERSION.zip
else
  mv libs-$VERSION.zip $DESTINATION
fi