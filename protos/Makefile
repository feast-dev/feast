.PHONY: go

dirs = core serving types storage
service_dirs = core serving

gen-go:
	@$(foreach dir,$(dirs),protoc -I/usr/local/include -I. --go_out=plugins=grpc,paths=source_relative:../sdk/go/protos/ feast/$(dir)/*.proto;)

gen-python:
	pip install grpcio-tools
	pip install mypy-protobuf
	@$(foreach dir,$(dirs),python -m grpc_tools.protoc -I. --python_out=../sdk/python/ --mypy_out=../sdk/python/ feast/$(dir)/*.proto;)
	@$(foreach dir,$(service_dirs),python -m grpc_tools.protoc -I. --grpc_python_out=../sdk/python/ feast/$(dir)/*.proto;)

gen-docs:
	protoc --doc_out=../docs/api/ --doc_opt=../docs/assets/protoc-gen-doc-markdown.tmpl,proto.md feast/core/*.proto feast/serving/*.proto feast/storage/*.proto feast/types/*.proto;