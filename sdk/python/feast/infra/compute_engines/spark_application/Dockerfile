FROM apache/spark:4.0.1

USER root

RUN apt-get update && apt-get install --no-install-suggests --no-install-recommends --yes git && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
ENV SPARK_HOME="/opt/spark"

WORKDIR /app

COPY sdk/python/feast/infra/compute_engines/spark_application/main.py /opt/feast/main.py

# Copy necessary parts of the Feast codebase
COPY sdk/python sdk/python
COPY protos protos
COPY pyproject.toml pyproject.toml
COPY README.md README.md

# setuptools_scm needs .git to infer the version.
# https://github.com/pypa/setuptools_scm#usage-from-docker
RUN --mount=source=.git,target=.git,type=bind uv pip install --system --no-cache-dir '.[redis,grpcio,k8s]' pymysql

# Hadoop AWS JARs for S3A filesystem support + AWS SDK v2 bundle
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o /opt/spark/jars/hadoop-aws-3.4.1.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.782/aws-java-sdk-bundle-1.12.782.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.782.jar && \
    curl -sL https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.4/bundle-2.28.4.jar \
    -o /opt/spark/jars/bundle-2.28.4.jar

USER spark
