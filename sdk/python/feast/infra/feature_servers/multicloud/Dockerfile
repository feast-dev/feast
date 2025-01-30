FROM registry.access.redhat.com/ubi8/python-311:1

ARG VERSION
RUN pip install "feast[aws,gcp,snowflake,redis,go,mysql,postgres,opentelemetry,grpcio,k8s,duckdb,milvus]"==${VERSION}

# modify permissions to support running with a random uid
RUN chmod g+w $(python -c "import feast.ui as ui; print(ui.__path__)" | tr -d "[']")/build/projects-list.json
