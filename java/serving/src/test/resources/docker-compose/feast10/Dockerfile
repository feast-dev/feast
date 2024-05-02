FROM python:3.11

WORKDIR /app
COPY sdk/python /mnt/feast/sdk/python
COPY protos /mnt/feast/protos
COPY setup.py /mnt/feast/setup.py
COPY pyproject.toml /mnt/feast/pyproject.toml
COPY README.md /mnt/feast/README.md
COPY Makefile /mnt/feast/Makefile
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0
RUN pip install uv
RUN cd /mnt/feast && uv pip install --system .[grpcio,redis]
COPY java/serving/src/test/resources/docker-compose/feast10/ .
EXPOSE 8080

CMD ["./entrypoint.sh"]
