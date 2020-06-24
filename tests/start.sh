#!/bin/bash

set -e

# This script is starting Long Running Tests on AKS
# by creating a CronJob that fires in every 10 minutes

# CronJob uses 'jupyter' image to run tests

LIGHT_GREEN='\033[1;32m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# FQDN of the ACR and name of the image; example, fffeastadopacr.azurecr.io/fffeast-jupyter
ACR_REPOSITORY=$1
# Tag of the Jupyter image; example, v56b9e30332cb08bde55f1d5f966d5d73515d711e
ACR_IMAGE_TAG=$2

CRON_JOB_ID=$3

if [ -z "$CRON_JOB_ID" ]
then
  CRON_JOB_ID=`python -c 'import time; print int(time.time())'`
fi

TEMPLATE="apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: long-running-tests-$CRON_JOB_ID
spec:
  concurrencyPolicy: Forbid
  schedule: \"*/10 * * * *\"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: jupyter
            image: $ACR_REPOSITORY:$ACR_IMAGE_TAG
          restartPolicy: Never"

echo "$TEMPLATE" | kubectl apply -f -

printf "Long Running Tests are started, here is the ${GREEN}Cron Job ID: ${LIGHT_GREEN}$CRON_JOB_ID${NC}\n\n"

printf "You can stop this session by calling the following command\n\n"

printf "${YELLOW}./stop.sh $CRON_JOB_ID${NC}\n"
