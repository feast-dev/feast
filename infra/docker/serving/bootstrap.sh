#!/bin/bash

CONFIG_PATH_FLAG=""
if [ -z "$CONFIG_PATH" ]; then
CONFIG_PATH_FLAG="-Dspring.config.additional-location=${CONFIG_PATH}"
fi

java $CONFIG_PATH_FLAG \
  -XX:+UnlockExperimentalVMOptions \
  -XX:+UseCGroupMemoryLimitForHeap \
  -XX:+UseStringDeduplication \
  -XX:+UseG1GC \
  -jar /usr/share/feast/feast-serving.jar