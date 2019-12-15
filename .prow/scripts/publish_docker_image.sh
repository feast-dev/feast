#!/usr/bin/env bash

set -e 
set -o pipefail

usage()
{
    echo "usage: publish_docker_images.sh

    --repository  the target repository to upload the Docker image, example:
                  gcr.io/kf-feast/feast-core

    --tag         the tag for the Docker image, example: 1.0.4

    --file        path to the Dockerfile

    [--google_service_account_file  
    path to Google Cloud service account JSON key file]
"
}

while [ "$1" != "" ]; do
  case "$1" in
      --repository )           REPOSITORY="$2";         shift;;
      --tag        )           TAG="$2";                shift;;
      --file       )           FILE="$2";               shift;;
      --google_service_account_file ) GOOGLE_SERVICE_ACCOUNT_FILE="$2";        shift;;
      -h | --help )            usage;                   exit;; 
      * )                      usage;                   exit 1
  esac
  shift
done

if [ -z $REPOSITORY ]; then usage; exit 1; fi
if [ -z $TAG ]; then usage; exit 1; fi
if [ -z $FILE ]; then usage; exit 1; fi
  
if [ $GOOGLE_SERVICE_ACCOUNT_FILE ]; then 
    gcloud -q auth activate-service-account --key-file $GOOGLE_SERVICE_ACCOUNT_FILE
    gcloud -q auth configure-docker
fi

echo "============================================================"
echo "Building Docker image $REPOSITORY:$TAG"
echo "============================================================"
docker build -t $REPOSITORY:$TAG --build-arg REVISION=$TAG -f $FILE .

echo "============================================================"
echo "Pushing Docker image $REPOSITORY:$TAG"
echo "============================================================"
docker push $REPOSITORY:$TAG
