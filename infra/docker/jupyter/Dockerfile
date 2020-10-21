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

ENV FEAST_SPARK_LAUNCHER standalone
ENV FEAST_SPARK_STANDALONE_MASTER "local[*]"
ENV FEAST_SPARK_HOME $SPARK_HOME
ENV FEAST_SPARK_EXTRA_OPTIONS "--jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar \
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"

CMD ["start-notebook.sh", "--NotebookApp.token=''"]