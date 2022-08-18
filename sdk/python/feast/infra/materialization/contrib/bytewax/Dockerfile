FROM python:3.9-slim-bullseye AS build

RUN apt-get update && \
    apt-get install --no-install-suggests --no-install-recommends --yes git

WORKDIR /bytewax

# Copy dataflow code
COPY sdk/python/feast/infra/materialization/contrib/bytewax/bytewax_materialization_dataflow.py /bytewax
COPY sdk/python/feast/infra/materialization/contrib/bytewax/dataflow.py /bytewax

# Copy entrypoint
COPY sdk/python/feast/infra/materialization/contrib/bytewax/entrypoint.sh /bytewax

# Copy necessary parts of the Feast codebase
COPY sdk/python sdk/python
COPY protos protos
COPY go go
COPY setup.py setup.py
COPY pyproject.toml pyproject.toml
COPY README.md README.md

# Install Feast for AWS with Bytewax dependencies
# We need this mount thingy because setuptools_scm needs access to the
# git dir to infer the version of feast we're installing.
# https://github.com/pypa/setuptools_scm#usage-from-docker
# I think it also assumes that this dockerfile is being built from the root of the directory.
RUN --mount=source=.git,target=.git,type=bind pip3 install --no-cache-dir -e '.[aws,gcp,bytewax]'

