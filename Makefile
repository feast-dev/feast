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

format: format-python format-go format-java

lint: lint-python lint-go lint-java

test: test-python test-java test-go

protos: compile-protos-go compile-protos-python compile-protos-docs

build: protos build-java build-docker build-html

install-ci-dependencies: install-python-ci-dependencies install-go-ci-dependencies install-java-ci-dependencies

# Java

install-java-ci-dependencies:
	mvn verify clean --fail-never

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
	cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --python_out=../sdk/python/ --mypy_out=../sdk/python/ tensorflow_metadata/proto/v0/*.proto

install-python: compile-protos-python
	pip install -e sdk/python --upgrade

test-python:
	pytest --verbose --color=yes sdk/python/tests

format-python:
	cd ${ROOT_DIR}/sdk/python; isort -rc feast tests
	cd ${ROOT_DIR}/sdk/python; black --target-version py37 feast tests

lint-python:
	# TODO: This mypy test needs to be re-enabled and all failures fixed
	#cd ${ROOT_DIR}/sdk/python; mypy feast/ tests/
	cd ${ROOT_DIR}/sdk/python; flake8 feast/ tests/
	cd ${ROOT_DIR}/sdk/python; black --check feast tests

# Go SDK

install-go-ci-dependencies:
	go get -u github.com/golang/protobuf/protoc-gen-go
	go get -u golang.org/x/lint/golint

compile-protos-go: install-go-ci-dependencies
	cd ${ROOT_DIR}/protos; protoc -I/usr/local/include -I. --go_out=plugins=grpc,paths=source_relative:../sdk/go/protos/ tensorflow_metadata/proto/v0/*.proto
	$(foreach dir,types serving core storage,cd ${ROOT_DIR}/protos; protoc -I/usr/local/include -I. --go_out=plugins=grpc,paths=source_relative:../sdk/go/protos feast/$(dir)/*.proto;)

test-go:
	cd ${ROOT_DIR}/sdk/go; go test ./...

format-go:
	cd ${ROOT_DIR}/sdk/go; gofmt -s -w *.go

lint-go:
	cd ${ROOT_DIR}/sdk/go; go vet

# Docker

build-push-docker:
	@$(MAKE) build-docker registry=$(REGISTRY) version=$(VERSION)
	@$(MAKE) push-core-docker registry=$(REGISTRY) version=$(VERSION)
	@$(MAKE) push-serving-docker registry=$(REGISTRY) version=$(VERSION)
	@$(MAKE) push-ci-docker registry=$(REGISTRY)
	
build-docker: build-core-docker build-serving-docker build-ci-docker

push-core-docker:
	docker push $(REGISTRY)/feast-core:$(VERSION)

push-serving-docker:
	docker push $(REGISTRY)/feast-serving:$(VERSION)

push-ci-docker:
	docker push $(REGISTRY)/feast-ci:latest

build-core-docker:
	docker build -t $(REGISTRY)/feast-core:$(VERSION) -f infra/docker/core/Dockerfile .

build-serving-docker:
	docker build -t $(REGISTRY)/feast-serving:$(VERSION) -f infra/docker/serving/Dockerfile .

build-ci-docker:
	docker build -t $(REGISTRY)/feast-ci:latest -f infra/docker/ci/Dockerfile .

# Documentation

install-dependencies-proto-docs:
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
	cd ${ROOT_DIR}/protos; protoc --docs_out=../dist/grpc feast/*/*.proto || \
	cd ${ROOT_DIR}; $(MAKE) install-dependencies-proto-docs && 	cd ${ROOT_DIR}/protos; PATH=$$HOME/bin:$$PATH protoc -I $$HOME/include/ -I . --docs_out=../dist/grpc feast/*/*.proto

clean-html:
	rm -rf 	$(ROOT_DIR)/dist

build-html: clean-html
	mkdir -p $(ROOT_DIR)/dist/python
	mkdir -p $(ROOT_DIR)/dist/grpc

	# Build Protobuf documentation
	$(MAKE) compile-protos-docs

	# Build Python SDK documentation
	$(MAKE) compile-protos-python
	cd 	$(ROOT_DIR)/sdk/python/docs && $(MAKE) html
	cp -r $(ROOT_DIR)/sdk/python/docs/html/* $(ROOT_DIR)/dist/python