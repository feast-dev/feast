# 
#  Copyright 2019 The Feast Authors
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      https://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
PROTO_TYPE_SUBDIRS = core serving types storage
PROTO_SERVICE_SUBDIRS = core serving

# General

format: format-python lint-go lint-java

lint: lint-python lint-go lint-java

test: test-python test-java

protos: compile-protos-go compile-protos-python compile-protos-docs

build: protos build-java build-docker build-html

# Java

format-java:
	mvn spotless:apply

lint-java:
	mvn spotless:check

test-java:
	mvn test

build-java:
	mvn clean verify

# Python SDK

install-python-ci-dependencies:
	pip install -r sdk/python/requirements-ci.txt

compile-protos-python: install-python-ci-dependencies
	@$(foreach dir,$(PROTO_TYPE_SUBDIRS),cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --python_out=../sdk/python/ --mypy_out=../sdk/python/ feast/$(dir)/*.proto;)
	@$(foreach dir,$(PROTO_SERVICE_SUBDIRS),cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --grpc_python_out=../sdk/python/ feast/$(dir)/*.proto;)

test-python:
	pytest --verbose --color=yes sdk/python/tests

format-python:
	cd ${ROOT_DIR}/sdk/python; isort -rc feast tests
	cd ${ROOT_DIR}/sdk/python; black --target-version py37 feast tests

lint-python:
	# TODO: This mypy test needs to be re-enabled and all failures fixed
	# cd ${ROOT_DIR}/sdk/python; mypy feast/ tests/
	cd ${ROOT_DIR}/sdk/python; flake8 feast/ tests/
	cd ${ROOT_DIR}/sdk/python; black --check feast tests
	cd ${ROOT_DIR}/sdk/python; isort -rc feast tests --check-only

# Go SDK

compile-protos-go:
	@$(foreach dir,$(PROTO_TYPE_SUBDIRS), cd ${ROOT_DIR}/protos; protoc -I/usr/local/include -I. --go_out=plugins=grpc,paths=source_relative:../sdk/go/protos/ feast/$(dir)/*.proto;)

format-go:
	cd ${ROOT_DIR}/sdk/go; gofmt -s -w *.go

lint-go:
	cd ${ROOT_DIR}/sdk/go; go vet; golint *.go

# Docker

build-docker:
	docker build -t $(REGISTRY)/feast-core:$(VERSION) -f infra/docker/core/Dockerfile .
	docker build -t $(REGISTRY)/feast-serving:$(VERSION) -f infra/docker/serving/Dockerfile .

build-push-docker:
	@$(MAKE) build-docker registry=$(REGISTRY) version=$(VERSION)
	docker push $(REGISTRY)/feast-core:$(VERSION)
	docker push $(REGISTRY)/feast-serving:$(VERSION)

# Documentation

install-dependencies-proto-docs:
	# Use the following command to compile dependencies if installing using the below method.
	# cd ${ROOT_DIR}/protos; PATH=$$HOME/bin:$$PATH protoc -I $$HOME/include/ \
	# -I . --docs_out=../dist/grpc feast/*/*.proto
	cd ${ROOT_DIR}/protos;
	mkdir -p $$HOME/bin
	mkdir -p $$HOME/include
	go get github.com/golang/protobuf/proto && \
	go get github.com/russross/blackfriday/v2 && \
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

compile-protos-docs:
	cd ${ROOT_DIR}/protos; protoc --docs_out=../dist/grpc feast/*/*.proto

clean-html:
	rm -rf 	$(ROOT_DIR)/dist

build-html: clean-html
	mkdir -p $(ROOT_DIR)/dist/python
	mkdir -p $(ROOT_DIR)/dist/grpc
	cd 	$(ROOT_DIR)/protos && $(MAKE) gen-docs
	cd 	$(ROOT_DIR)/sdk/python/docs && $(MAKE) html
	cp -r $(ROOT_DIR)/sdk/python/docs/html/* $(ROOT_DIR)/dist/python