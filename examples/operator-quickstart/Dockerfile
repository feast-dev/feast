FROM quay.io/jupyter/scipy-notebook:python-3.11

USER root

RUN chgrp -R 0 /home/jovyan && \
    chmod -R g=u /home/jovyan

USER jovyan