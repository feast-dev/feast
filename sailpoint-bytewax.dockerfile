# Original File: sdk/python/feast/infra/materialization/contrib/bytewax/Dockerfile
FROM 406205545357.dkr.ecr.us-east-1.amazonaws.com/sailpoint/python3.10:1 AS build

RUN dnf install --refresh -y git

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
RUN --mount=source=.git,target=.git,type=bind SETUPTOOLS_SCM_PRETEND_VERSION=1 \
pip3 install setuptools==69.* pip==24.* --upgrade --no-cache-dir \
'.[aws,gcp,bytewax,snowflake,postgres,grpcio]'
