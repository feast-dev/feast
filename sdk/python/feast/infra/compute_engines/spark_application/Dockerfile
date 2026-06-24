FROM apache/spark:4.0.1

USER root

RUN pip install --no-cache-dir \
    "feast[redis]==0.64.0" \
    pyyaml \
    kubernetes \
    tqdm

# Hadoop AWS JARs for S3A filesystem support + AWS SDK v2 bundle
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o /opt/spark/jars/hadoop-aws-3.4.1.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.782/aws-java-sdk-bundle-1.12.782.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.782.jar && \
    curl -sL https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.4/bundle-2.28.4.jar \
    -o /opt/spark/jars/bundle-2.28.4.jar

COPY feast/infra/compute_engines/spark_application/main.py /opt/feast/main.py

USER spark
