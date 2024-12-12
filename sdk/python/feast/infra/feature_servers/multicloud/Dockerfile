FROM registry.access.redhat.com/ubi9/python-311:9.5

ARG VERSION

RUN mkdir -p ~/miniconda3 && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-py311_24.9.2-0-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh && \
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3 && \
    rm ~/miniconda3/miniconda.sh

RUN source ~/miniconda3/bin/activate && \
    conda config --add channels conda-forge && \
    # update all packages to make sure they're from conda-forge, not defaults
    conda update --all && \
    conda install -c conda-forge pyarrow=17 libarrow-flight && \
    conda clean -a
    
RUN pip install --no-cache-dir "feast[aws,gcp,snowflake,redis,go,mysql,postgres,opentelemetry,grpcio,k8s]"==${VERSION}

# modify permissions to support running with a random uid
RUN chmod g+w $(python -c "import feast.ui as ui; print(ui.__path__)" | tr -d "[']")/build/projects-list.json
