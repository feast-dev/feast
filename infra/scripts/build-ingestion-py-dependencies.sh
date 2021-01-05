#!/usr/bin/env bash
set -euo pipefail
PLATFORM=$1
DESTINATION=$2
PACKAGES=${PACKAGES:-"great-expectations==0.13.2 pyarrow==2.0.0 datadog==0.39.0"}

tmp_dir=$(mktemp -d)

pip3 install -t ${tmp_dir}/libs $PACKAGES

cd $tmp_dir
tar -czf pylibs-ge-$PLATFORM.tar.gz libs/
if [[ $DESTINATION == gs* ]]; then
  gsutil cp pylibs-ge-$PLATFORM.tar.gz $DESTINATION
else
  mv pylibs-ge-$PLATFORM.tar.gz $DESTINATION
fi
