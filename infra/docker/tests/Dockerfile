ARG BASE_IMAGE=gcr.io/kf-feast/feast-ci:latest

FROM ${BASE_IMAGE}

RUN mkdir -p /src/sdk /src/spark/ingestion

COPY sdk/python /src/sdk/python

COPY README.md /src/README.md

WORKDIR /src

RUN pip install -r sdk/python/requirements-ci.txt

RUN git init .
RUN pip install -e sdk/python -U
RUN pip install "s3fs" "boto3" "urllib3>=1.25.4"

COPY tests /src/tests

RUN pip install -r tests/requirements.txt

COPY infra/scripts /src/infra/scripts
COPY spark/ingestion/target /src/spark/ingestion/target
