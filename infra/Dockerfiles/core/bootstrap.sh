#!/bin/bash

CONFIG_PATH_FLAG=""
if [ -z "$CONFIG_PATH" ]; then
CONFIG_PATH_FLAG="-Dspring.config.additional-location=${CONFIG_PATH}"
fi

java $CONFIG_PATH_FLAG \
  -XX:+UnlockExperimentalVMOptions \
  -XX:+UseCGroupMemoryLimitForHeap \
  -jar /usr/share/feast/feast-core.jar