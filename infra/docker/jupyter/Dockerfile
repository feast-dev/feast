FROM jupyter/pyspark-notebook:ae5f7e104dd5

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

# Switch back to original user and workdir
USER $NB_UID
WORKDIR $HOME

COPY examples .

CMD ["start-notebook.sh", "--NotebookApp.token=''"]