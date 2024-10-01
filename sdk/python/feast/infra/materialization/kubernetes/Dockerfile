FROM debian:11-slim AS build

RUN apt-get update && \
    apt-get install --no-install-suggests --no-install-recommends --yes git python3 python3-pip

WORKDIR /app

COPY sdk/python/feast/infra/materialization/kubernetes/main.py /app

# Copy necessary parts of the Feast codebase
COPY sdk/python sdk/python
COPY protos protos
COPY go go
COPY setup.py setup.py
COPY pyproject.toml pyproject.toml
COPY README.md README.md

# We need this mount thingy because setuptools_scm needs access to the
# git dir to infer the version of feast we're installing.
# https://github.com/pypa/setuptools_scm#usage-from-docker
# I think it also assumes that this dockerfile is being built from the root of the directory.
RUN --mount=source=.git,target=.git,type=bind pip3 install --no-cache-dir '.[aws,gcp,k8s,snowflake,postgres]'
