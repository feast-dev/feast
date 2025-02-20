FROM registry.access.redhat.com/ubi8/python-311:1
# keep VERSION arg on line #3, this is critical to release CI
ARG VERSION="0.46.0"

RUN pip install "feast[aws,gcp,snowflake,redis,go,mysql,postgres,opentelemetry,grpcio,k8s,duckdb,milvus]"==${VERSION}

# modify permissions to support running with a random uid
RUN chmod g+w $(python -c "import feast.ui as ui; print(ui.__path__)" | tr -d "[']")/build/projects-list.json
