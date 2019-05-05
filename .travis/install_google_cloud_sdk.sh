#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
GOOGLE_CLOUD_SDK_ARCHIVE_URL=https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-244.0.0-linux-x86_64.tar.gz

wget -qO- ${GOOGLE_CLOUD_SDK_ARCHIVE_URL} | tar xz
export PATH=$PWD/google-cloud-sdk/bin:$PATH
gcloud -q components install kubectl
gcloud config set project kf-feast
gcloud -q auth activate-service-account --key-file=${SCRIPT_DIR}/service_account.json
gcloud -q auth configure-docker
gcloud -q container clusters get-credentials feast-test-cluster --zone us-central1-a --project kf-feast