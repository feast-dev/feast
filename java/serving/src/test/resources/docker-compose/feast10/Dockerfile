FROM python:3.11

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY sdk/python /mnt/feast/sdk/python
COPY protos /mnt/feast/protos
COPY pyproject.toml /mnt/feast/pyproject.toml
COPY README.md /mnt/feast/README.md
COPY Makefile /mnt/feast/Makefile
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0
RUN cd /mnt/feast && uv pip install --system .[grpcio,redis]
COPY java/serving/src/test/resources/docker-compose/feast10/ .
EXPOSE 8080

CMD ["./entrypoint.sh"]
