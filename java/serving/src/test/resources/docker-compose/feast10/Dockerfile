FROM python:3.11

WORKDIR /app
COPY java/serving/src/test/resources/docker-compose/feast10/ .
COPY sdk/python /mnt/feast/sdk/python
COPY protos /mnt/feast/protos
COPY setup.py /mnt/feast/setup.py
COPY pyproject.toml /mnt/feast/pyproject.toml
COPY README.md /mnt/feast/README.md
RUN cd /mnt/feast && SETUPTOOLS_SCM_PRETEND_VERSION="0.1.0" pip install .[grpcio,redis]
EXPOSE 8080

CMD ["./entrypoint.sh"]
