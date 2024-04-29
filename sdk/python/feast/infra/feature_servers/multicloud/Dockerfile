FROM python:3.11

RUN apt update && \
        apt install -y \
        jq \
        python3-dev \
        build-essential

RUN pip install pip --upgrade
RUN pip install "feast[aws,gcp,snowflake,redis,go,mysql,postgres]"


RUN apt update
RUN apt install -y -V ca-certificates lsb-release wget
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt update
RUN apt -y install libarrow-dev
RUN mkdir -m 775 /.cache