#!/bin/bash

PROJECT_ID=$1
DATASET_NAME=$2
for i in $(bq ls $PROJECT_ID:$DATASET_NAME | sed 1,2d | awk "{print \$1}");
do
  if  [[ ! -z "$i" ]]; then
    bq rm -ft $PROJECT_ID:$DATASET_NAME.$i;
  fi
done