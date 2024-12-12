FROM quay.io/jupyter/scipy-notebook:python-3.11

USER root

RUN chgrp -R 0 /home/jovyan && \
    chmod -R g=u /home/jovyan

USER jovyan

RUN conda install -c conda-forge pyarrow libarrow-flight
RUN pip install --no-cache-dir "feast[aws,gcp,snowflake,redis,go,mysql,postgres,opentelemetry,grpcio,k8s]"==0.42.0