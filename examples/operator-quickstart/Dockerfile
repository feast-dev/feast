FROM feastdev/feature-server:0.42.0

RUN pip install --no-cache-dir "feast[aws,gcp,snowflake,redis,go,mysql,postgres,opentelemetry,grpcio,k8s,duckdb]" \
    streamlit \
    shap \
    pandas \
    scikit-learn \
    matplotlib

RUN pwd
ADD https://github.com/feast-dev/feast-credit-score-local-tutorial/archive/f43b44b.tar.gz /
COPY feature_store.yaml /
RUN chgrp 0 /f43b44b.tar.gz /feature_store.yaml && chmod g=u /f43b44b.tar.gz /feature_store.yaml
RUN ls -la /

# kubectl expose deploy/feast-example --port 80 --target-port 6566 --name feast-example-online
# kubectl expose deploy/feast-example --port 80 --target-port 6570 --name feast-example-registry
# kubectl expose deploy/feast-example --port 80 --target-port 8815 --name feast-example-offline 
