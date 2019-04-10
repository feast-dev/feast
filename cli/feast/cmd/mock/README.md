Mock GRPC client is created using this library
https://github.com/golang/mock

An example to generate `mock_core.go`:

```
cd $FEAST_REPO/cli/feast
mockgen --package mock github.com/gojek/feast/protos/generated/go/feast/core CoreServiceClient > cmd/mock/core_service.go
```

> We need to re-run `mockgen` whenever the protos for the services are updated