FROM jupyter/pyspark-notebook:399cbb986c6b

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
RUN git init .
COPY README.md README.md
RUN pip install feast~=0.9 -U
RUN pip install "s3fs" "boto3" "urllib3>=1.25.4"

# Switch back to original user and workdir
USER $NB_UID
WORKDIR $HOME

COPY examples .

ENV FEAST_SPARK_LAUNCHER standalone
ENV FEAST_SPARK_STANDALONE_MASTER "local[*]"
ENV FEAST_SPARK_HOME $SPARK_HOME

CMD ["start-notebook.sh", "--NotebookApp.token=''"]