# Flink/PyFlink versions must stay aligned: the base image's Flink version, the
# apache-flink wheel, and the engine's `flink_version` / `python_jar_uri` config
# must all reference the same release. They are pinned to 2.2.1 here (the `flink`
# extra floor is apache-flink>=2.2.1); bump the base image, the apache-flink pin
# below, and the config defaults together.
# pemja (a PyFlink dependency) ships only linux/amd64 wheels — build for amd64.
FROM flink:2.2.1

USER root

RUN apt-get update && apt-get install --no-install-suggests --no-install-recommends --yes \
    git python3 python3-pip python3-venv && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY sdk/python/feast/infra/compute_engines/flink_application/main.py /opt/feast/main.py

# Copy necessary parts of the Feast codebase
COPY sdk/python sdk/python
COPY protos protos
COPY pyproject.toml pyproject.toml
COPY README.md README.md

# setuptools_scm needs .git to infer the version.
# https://github.com/pypa/setuptools_scm#usage-from-docker
# apache-flink is pinned to the base image's Flink release so the PyFlink wheel
# matches the distribution and the flink-python jar (the `flink` extra alone
# would float to the newest 2.2.x).
RUN --mount=source=.git,target=.git,type=bind uv pip install --system --break-system-packages \
    --no-cache-dir '.[redis,grpcio,k8s,flink]' 'apache-flink==2.2.1' pymysql

USER flink
