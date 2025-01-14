FROM registry.access.redhat.com/ubi9/python-311:9.5

ARG VERSION
RUN pip install "feast[aws,gcp,snowflake,redis,go,mysql,postgres,opentelemetry,grpcio,k8s,duckdb,milvus]"==${VERSION}

# modify permissions to support running with a random uid
RUN chmod g+w $(python -c "import feast.ui as ui; print(ui.__path__)" | tr -d "[']")/build/projects-list.json
