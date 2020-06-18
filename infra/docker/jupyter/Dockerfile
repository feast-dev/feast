FROM jupyter/minimal-notebook:619e9cc2fc07

USER root

WORKDIR /feast

# Init empty .git settings (required by SDK setup.py)
RUN git init .

COPY sdk/python sdk/python
COPY Makefile Makefile
COPY protos protos
COPY README.md README.md

# Install Python dependencies
RUN make compile-protos-python

# Install CI requirements (only needed for running tests)
RUN pip install -r sdk/python/requirements-ci.txt

# Install Feast SDK
RUN pip install -e sdk/python -U

# Switch back to original user and workdir
USER $NB_UID
WORKDIR $HOME

CMD start-notebook.sh --NotebookApp.token=''
