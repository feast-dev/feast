.PHONY: go

dirs = core serving specs storage types
service_dirs = core serving

gen-go:
	@$(foreach dir,$(dirs),protoc -I/usr/local/include -I. --go_out=plugins=grpc:$$GOPATH/src feast/$(dir)/*.proto;)

gen-python:
	pip install grpcio-tools
	@$(foreach dir,$(dirs),python -m grpc_tools.protoc -I. --python_out=../sdk/python/ feast/$(dir)/*.proto;)
	@$(foreach dir,$(service_dirs),python -m grpc_tools.protoc -I. --grpc_python_out=../sdk/python/ feast/$(dir)/*.proto;)