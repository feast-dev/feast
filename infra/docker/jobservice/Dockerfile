FROM jupyter/pyspark-notebook:ae5f7e104dd5

USER root
WORKDIR /feast

COPY sdk/python sdk/python
COPY Makefile Makefile
COPY protos protos

# Install necessary tools for later steps
RUN apt-get update && apt-get -y install make git wget

# Install Python dependencies
RUN make compile-protos-python

# Install Feast SDK
COPY .git .git
COPY README.md README.md
RUN pip install -e sdk/python -U
RUN pip install "s3fs" "boto3" "urllib3>=1.25.4"

#
# Download grpc_health_probe to run health checks
# https://kubernetes.io/blog/2018/10/01/health-checking-grpc-servers-on-kubernetes/
#
RUN wget -q https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-amd64 \
         -O /usr/bin/grpc-health-probe && \
    chmod +x /usr/bin/grpc-health-probe

ENV FEAST_SPARK_LAUNCHER standalone
ENV FEAST_SPARK_STANDALONE_MASTER "local[*]"
ENV FEAST_SPARK_HOME $SPARK_HOME

CMD ["feast", "server"]
