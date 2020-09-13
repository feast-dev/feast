FROM jupyter/minimal-notebook:619e9cc2fc07

USER root
WORKDIR /feast

COPY sdk/python sdk/python
COPY Makefile Makefile
COPY protos protos
COPY tests tests

# Install Python dependencies
RUN make compile-protos-python

# Install CI requirements (only needed for running tests)
RUN pip install -r sdk/python/requirements-ci.txt

# Install Feast SDK
COPY .git .git
COPY README.md README.md
RUN pip install -e sdk/python -U

# Copy examples and set workdir
USER $NB_UID
WORKDIR $HOME
COPY examples .

USER root
COPY infra/docker/jupyter/entrypoint.sh .
CMD ["bash", "./entrypoint.sh", "--NotebookApp.token=''"]
