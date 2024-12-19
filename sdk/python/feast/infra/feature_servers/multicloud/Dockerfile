FROM python:3.11-slim-bullseye

RUN pip install --no-cache-dir pip --upgrade
RUN pip install --no-cache-dir "feast[aws,gcp,snowflake,redis,go,mysql,postgres,opentelemetry,grpcio,k8s,duckdb]"


RUN apt update && apt install -y -V ca-certificates lsb-release wget && \
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && apt update && \
    apt install -y \
    jq \
    libarrow-dev && \
    apt remove -y lsb-release wget && \
    apt-get clean && rm -rf /var/cache/apt/lists

# modify permissions to support running with a random uid
RUN mkdir -m 775 /.cache
RUN chmod g+w $(python3 -c "import feast.ui as _; print(_.__path__)" | tr -d "[']")/build/projects-list.json
