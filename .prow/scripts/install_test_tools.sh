#!/usr/bin/env bash
set -e

# This script installs the following Feast test utilities:
# ============================================================
# - gettext package so we can use envsubst command to provide values to helm template file
# - Python 3.6 because Feast requires Python version 3.6 and above
# - Golang if we need to build Feast CLI from source
# - Helm if we want to install Feast release

apt-get -qq update 
apt-get -y install curl wget gettext &> /dev/null

curl -s https://repo.continuum.io/miniconda/Miniconda3-4.5.12-Linux-x86_64.sh -o /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p /miniconda &> /dev/null
export PATH=/miniconda/bin:$PATH

wget -qO- https://dl.google.com/go/go1.12.5.linux-amd64.tar.gz | tar xzf -
mv go /usr/local/
export PATH=/usr/local/go/bin:$PATH
export GO111MODULE=on

wget -qO- https://storage.googleapis.com/kubernetes-helm/helm-v2.13.1-linux-amd64.tar.gz | tar xz
mv linux-amd64/helm /usr/local/bin/helm
