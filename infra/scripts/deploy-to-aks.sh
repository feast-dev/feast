#!/bin/bash

# This script automates deploy Feast to Azure Kubernetes Service (a.k.a AKS)
# Checks dependencies first, if there is a missing dependency, install it silently

# Stop on error
set -e

# check kubectl

if ! [ -x "$(command -v kubectl)" ]; then
  echo 'Error: kubectl is not installed.' >&2

  curl -LO https://storage.googleapis.com/kubernetes-release/release/`curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt`/bin/linux/amd64/kubectl
  chmod +x ./kubectl
  sudo mv ./kubectl /usr/local/bin/kubectl
fi

az aks get-credentials --name feast-temp --resource-group farfetch_rg

# check helm

if ! [ -x "$(command -v helm)" ]; then
  echo 'Error: helm is not installed.' >&2

  curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash
fi

# update helm repos

helm repo update

# install required plugins

helm plugin install https://github.com/databus23/helm-diff --version master

# check helmsman

if ! [ -x "$(command -v helmsman)" ]; then
  echo 'Error: helmsman is not installed.' >&2

  curl -L https://github.com/Praqma/helmsman/releases/download/v3.2.0/helmsman_3.2.0_linux_amd64.tar.gz | tar zx

  chmod +x helmsman
fi

# run helmsman

./helmsman -apply -f ./infra/desired-state/desired-state.yaml -debug -no-ns -no-env-subst
