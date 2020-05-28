.PHONY: go

dirs = core serving types storage
service_dirs = core serving

gen-go:
	@$(foreach dir,$(dirs),protoc -I/usr/local/include -I. --go_out=plugins=grpc,paths=source_relative:../sdk/go/protos/ feast/$(dir)/*.proto;)

gen-python:
	pip install grpcio-tools
	pip install mypy-protobuf
	@$(foreach dir,$(dirs),python3 -m grpc_tools.protoc -I. --python_out=../sdk/python/ --mypy_out=../sdk/python/ feast/$(dir)/*.proto;)
	@$(foreach dir,$(service_dirs),python3 -m grpc_tools.protoc -I. --grpc_python_out=../sdk/python/ feast/$(dir)/*.proto;)

install-dependencies-docs:
	mkdir -p $$HOME/bin
	mkdir -p $$HOME/include
	go get github.com/golang/protobuf/proto && \
	go get gopkg.in/russross/blackfriday.v2 && \
	cd $$(mktemp -d) && \
	git clone https://github.com/istio/tools/ && \
	cd tools/cmd/protoc-gen-docs && \
	go build && \
	cp protoc-gen-docs $$HOME/bin && \
	cd $$HOME && curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.11.2/protoc-3.11.2-linux-x86_64.zip && \
	unzip protoc-3.11.2-linux-x86_64.zip -d protoc3 && \
	mv protoc3/bin/* $$HOME/bin/ && \
	chmod +x $$HOME/bin/protoc && \
	mv protoc3/include/* $$HOME/include

gen-docs:
	protoc --docs_out=../dist/grpc feast/*/*.proto || \
	$(MAKE) install-dependencies-docs && PATH=$$HOME/bin:$$PATH protoc -I $$HOME/include/ -I . --docs_out=../dist/grpc feast/*/*.proto
