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

PROJECT_ROOT 	:= $(shell git rev-parse --show-toplevel)

test:
	mvn test

test-integration:
	$(MAKE) -C testing/integration test-integration TYPE=$(TYPE) ID=$(ID)

build-proto:
	$(MAKE) -C protos gen-go
	$(MAKE) -C protos gen-python
	$(MAKE) -C protos gen-docs

build-cli:
	$(MAKE) build-proto
	$(MAKE) -C cli build-all

build-java:
	mvn clean verify

# Python SDK

install-python-ci-dependencies:
	pip install -r sdk/python/requirements-ci.txt

compile-protos-python: install-python-ci-dependencies
	@$(foreach dir,$(PROTO_TYPE_SUBDIRS),cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --python_out=../sdk/python/ --mypy_out=../sdk/python/ feast/$(dir)/*.proto;)
	@$(foreach dir,$(PROTO_SERVICE_SUBDIRS),cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --grpc_python_out=../sdk/python/ feast/$(dir)/*.proto;)
	cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --python_out=../sdk/python/ --mypy_out=../sdk/python/

install-python: compile-protos-python
	pip install -e sdk/python --upgrade

test-python:
	pytest --verbose --color=yes sdk/python/tests

build-docker:
	docker build -t $(REGISTRY)/feast-core:$(VERSION) -f infra/docker/core/Dockerfile .
	docker build -t $(REGISTRY)/feast-serving:$(VERSION) -f infra/docker/serving/Dockerfile .

build-push-docker:
	@$(MAKE) build-docker registry=$(REGISTRY) version=$(VERSION)
	docker push $(REGISTRY)/feast-core:$(VERSION)
	docker push $(REGISTRY)/feast-serving:$(VERSION)

clean-html:
	rm -rf 	$(PROJECT_ROOT)/dist

build-html:
	rm -rf $(PROJECT_ROOT)/dist/
	mkdir -p $(PROJECT_ROOT)/dist/python
	mkdir -p $(PROJECT_ROOT)/dist/grpc
	cd 	$(PROJECT_ROOT)/protos && $(MAKE) gen-docs
	cd 	$(PROJECT_ROOT)/sdk/python/docs && $(MAKE) html
	cp -r $(PROJECT_ROOT)/sdk/python/docs/html/* $(PROJECT_ROOT)/dist/python