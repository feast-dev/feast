FROM feastdev/feature-server:0.42.0

ADD https://github.com/feast-dev/feast-credit-score-local-tutorial/archive/f43b44b.tar.gz /
COPY feature_store.yaml /

RUN pip install --no-cache-dir "feast[aws,gcp,snowflake,redis,go,mysql,postgres,opentelemetry,grpcio,k8s,duckdb]" \
    streamlit \
    shap \
    pandas \
    scikit-learn \
    matplotlib
