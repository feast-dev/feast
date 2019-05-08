#!/usr/bin/env bash

wget -qO- https://storage.googleapis.com/kubernetes-helm/helm-v2.13.1-linux-amd64.tar.gz | tar xz
sudo mv linux-amd64/helm /usr/local/bin/helm