FROM python:3.6-alpine as builder
WORKDIR /build/feast-sdk
ADD . ./
RUN python setup.py sdist bdist_wheel

FROM jupyter/datascience-notebook:latest
COPY --from=builder /build/feast-sdk/dist /usr/local/lib/feast
RUN pip install -f /usr/local/lib/feast feast