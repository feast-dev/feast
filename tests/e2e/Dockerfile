ARG FEAST_CI_IMAGE

FROM ${FEAST_CI_IMAGE}

WORKDIR /feast

COPY Makefile .
COPY protos protos
COPY sdk/python sdk/python

# Compile feast.core Python protobuf module
# FIXME this is redundant with infra/docker/ci/Dockerfile
RUN make compile-protos-python

# Install Feast Python SDK and test requirements

COPY tests/e2e/requirements.txt tests/e2e/requirements.txt
COPY README.md README.md

# setup.py in sdk/python grabs the version number from the .git
# files, but we don't want to COPY .git so as to maximize
# layer reuse. Fool it by initialize an empty .git repository.
RUN git init .

RUN pip install -qe sdk/python && \
  pip install -qr tests/e2e/requirements.txt
