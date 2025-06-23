This directory contains the Go logic that's executed by the `EmbeddedOnlineFeatureServer` from Python.

## Build and Run
To build and run the Go Feature Server locally, create a feature_store.yaml file with necessary configurations and run below commands:

```bash
    go build -o feast ./go/main.go
    ./feast --type=http --port=8080
```

## Running Integration Tests

To run go Integration tests, run below command

```bash
    make test-go-integration
```