[Update 10/31/2024] This Go feature server code is updated from the Expedia Group's forked Feast branch (https://github.com/ExpediaGroup/feast.git) on 10/22/2024. Thanks the engineers of the Expedia Groups who contributed and improved the Go Feature Server.  


## Build and Run
To build and run the Go Feature Server locally, create a feature_store.yaml file with necessary configurations and run below commands:

```bash
    go build -o feast-go ./go/main.go
    # start the http server
    ./feast-go --type=http --port=8080
    # or start the gRPC server
    #./feast-go --type=grpc  --port=[your-choice]
```

## OTEL based observability
The OS level env variable `ENABLE_OTEL_TRACING=="true"/"false"` (string type) is used to enable/disable this service (with Tracing only).

The default export URL is "http://localhost:4318". The default schema of sending data to collector is **HTTP**. Please refer the following two docs about the configuration of the OTEL exporter:  
1. https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/  
2. https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp#WithEndpoint  

## List of files have OTEL observability code  
1. internal/feast/transformation/transformation.go

3. internal/feast/onlinestore/redisonlinestore.go
4. internal/feast/server/grpc_server.go
5. internal/feast/server/http_server.go
6. internal/feast/server/server_commons.go
7. internal/feast/featurestore.go

## Example monitoring infra setup
1. docker compose file to setup Prometheus, Jaeger, and OTEL-collector.  
```yaml
services:
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yaml:/etc/prometheus/prometheus.yaml
    ports:
      - 9090:9090   # web UI  http://localhost:9090
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - 16686:16686  # Web UI: http://localhost:16686
      - 14268:14268  # http based receiver
      - 14250:14250  # gRPC based receiver
  otel-collector:
    image: otel/opentelemetry-collector-contrib
    volumes:
      - ./otel-collector-config.yaml:/etc/otelcol-contrib/config.yaml
    ports:
      #- 1888:1888 # pprof extension
      - 8888:8888 # Prometheus metrics exposed by the Collector
      - 8889:8889 # Prometheus exporter metrics
      #- 13133:13133 # health_check extension
      #- 4317:4317 # OTLP gRPC receiver
      - 4318:4318 # OTLP http receiver
      - 55679:55679 # zpages extension. check http://localhost:55679/debug/tracez
    depends_on:
      - jaeger
      - prometheus
```  
2. OTEL collector configure file.  
```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
    namespace: feast-go
  otlp/jarger:  # this is a gRPC based exporter. use "otelhttp" for http based exporter.
    endpoint: jaeger:4317
    tls:
      insecure: true

extensions:
  zpages:
    endpoint: 0.0.0.0:55679

processors:
  batch:

service:
  extensions: [zpages]
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
    traces:
      receivers: [otlp]
      exporters: [otlp/jarger]
```
3. Prometheus config.
```yaml
 #https://github.com/prometheus/prometheus/blob/release-3.6/config/testdata/conf.good.yml
 scrape_configs:
  - job_name: 'otel-collector'
    scrape_interval: 1m
    scrape_timeout: 30s # Increase this if needed
    static_configs:
      # Check the IP address of or Docker host network. 
      # Refer: https://stackoverflow.com/questions/48546124/what-is-the-linux-equivalent-of-host-docker-internal
      - targets: ['172.17.0.1:8888'] # Replace with the Collector's IP and port
  - job_name: 'otel-collected'
    scrape_interval: 1m
    scrape_timeout: 30s # Increase this if needed
    static_configs:
      - targets: ['172.17.0.1:8889'] # Replace with the Collector's IP and port
```
4. Jaeger config file is not used in this setup.    