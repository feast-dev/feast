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
MVN := mvn -f java/pom.xml ${MAVEN_EXTRA_OPTS}
PROTO_TYPE_SUBDIRS = core serving types storage
PROTO_SERVICE_SUBDIRS = core serving
OS := linux
ifeq ($(shell uname -s), Darwin)
	OS = osx
endif

# General

format: format-python format-java format-go

lint: lint-python lint-java lint-go

test: test-python test-java test-go

protos: compile-protos-go compile-protos-python compile-protos-docs

build: protos build-java build-docker build-html

install-ci-dependencies: install-python-ci-dependencies install-java-ci-dependencies install-go-ci-dependencies

# Python SDK

install-python-ci-dependencies:
	pip install -e "sdk/python[ci]"

package-protos:
	cp -r ${ROOT_DIR}/protos ${ROOT_DIR}/sdk/python/feast/protos

compile-protos-python:
	@$(foreach dir,$(PROTO_TYPE_SUBDIRS),cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --grpc_python_out=../sdk/python/feast/protos/ --python_out=../sdk/python/feast/protos/ --mypy_out=../sdk/python/feast/protos/ feast/$(dir)/*.proto;)
	@$(foreach dir,$(PROTO_TYPE_SUBDIRS),grep -rli 'from feast.$(dir)' sdk/python/feast/protos | xargs -I@ sed -i.bak 's/from feast.$(dir)/from feast.protos.feast.$(dir)/g' @;)

install-python:
	python -m pip install -e sdk/python -U --use-deprecated=legacy-resolver

benchmark-python:
	FEAST_USAGE=False IS_TEST=True python -m pytest --integration --benchmark  --benchmark-autosave --benchmark-save-data sdk/python/tests

benchmark-python-local:
	FEAST_USAGE=False IS_TEST=True FEAST_IS_LOCAL_TEST=True python -m pytest --integration --benchmark  --benchmark-autosave --benchmark-save-data sdk/python/tests

test-python:
	FEAST_USAGE=False IS_TEST=True python -m pytest -n 8 sdk/python/tests

test-python-integration:
	FEAST_USAGE=False IS_TEST=True python -m pytest -n 8 --integration sdk/python/tests

test-python-universal-local:
	FEAST_USAGE=False IS_TEST=True FEAST_IS_LOCAL_TEST=True python -m pytest -n 8 --integration --universal sdk/python/tests

test-python-universal:
	FEAST_USAGE=False IS_TEST=True python -m pytest -n 8 --integration --universal sdk/python/tests

format-python:
	# Sort
	cd ${ROOT_DIR}/sdk/python; python -m isort feast/ tests/

	# Format
	cd ${ROOT_DIR}/sdk/python; python -m black --target-version py37 feast tests

lint-python:
	cd ${ROOT_DIR}/sdk/python; python -m mypy feast/ tests/
	cd ${ROOT_DIR}/sdk/python; python -m isort feast/ tests/ --check-only
	cd ${ROOT_DIR}/sdk/python; python -m flake8 feast/ tests/
	cd ${ROOT_DIR}/sdk/python; python -m black --check feast tests

# Java

install-java-ci-dependencies:
	${MVN} verify clean --fail-never

format-java:
	${MVN} spotless:apply

lint-java:
	${MVN} --no-transfer-progress spotless:check

test-java:
	${MVN} --no-transfer-progress -DskipITs=true test

test-java-integration:
	${MVN} --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -DskipUTs=true clean verify

test-java-with-coverage:
	${MVN} --no-transfer-progress -DskipITs=true test jacoco:report-aggregate

build-java:
	${MVN} clean verify

build-java-no-tests:
	${MVN} --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -DskipUTs=true -DskipITs=true -Drevision=${REVISION} clean package

# Go SDK

install-go-ci-dependencies:
	go get -u github.com/golang/protobuf/protoc-gen-go
	go get -u golang.org/x/lint/golint

compile-protos-go:
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
	@$(MAKE) push-ci-docker registry=$(REGISTRY) version=$(VERSION)
	@$(MAKE) push-core-docker registry=$(REGISTRY) version=$(VERSION)
	@$(MAKE) push-serving-docker registry=$(REGISTRY) version=$(VERSION)

build-docker: build-ci-docker build-core-docker build-serving-docker

push-ci-docker:
	docker push $(REGISTRY)/feast-ci:$(VERSION)

build-ci-docker:
	docker build -t $(REGISTRY)/feast-ci:$(VERSION) -f infra/docker/ci/Dockerfile .

build-local-test-docker:
	docker build -t feast:local -f infra/docker/tests/Dockerfile .

push-core-docker:
	docker push $(REGISTRY)/feast-core:$(VERSION)

push-serving-docker:
	docker push $(REGISTRY)/feast-serving:$(VERSION)

build-core-docker:
	docker build --build-arg VERSION=$(VERSION) -t $(REGISTRY)/feast-core:$(VERSION) -f infra/docker/core/Dockerfile .

build-serving-docker:
	docker build --build-arg VERSION=$(VERSION) -t $(REGISTRY)/feast-serving:$(VERSION) -f infra/docker/serving/Dockerfile .

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
	cd $$HOME && curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.11.2/protoc-3.11.2-${OS}-x86_64.zip && \
	unzip protoc-3.11.2-${OS}-x86_64.zip -d protoc3 && \
	mv protoc3/bin/* $$HOME/bin/ && \
	chmod +x $$HOME/bin/protoc && \
	mv protoc3/include/* $$HOME/include

compile-protos-docs:
	rm -rf 	$(ROOT_DIR)/dist/grpc
	mkdir -p dist/grpc;
	cd ${ROOT_DIR}/protos && protoc --docs_out=../dist/grpc feast/*/*.proto

build-sphinx: compile-protos-python
	cd 	$(ROOT_DIR)/sdk/python/docs && $(MAKE) build-api-source

build-templates:
	python infra/scripts/compile-templates.py


# Java Docker

build-push-java-docker:
	@$(MAKE) build-feature-server-java-docker registry=$(REGISTRY) version=$(VERSION)
	@$(MAKE) push-feature-server-java-docker registry=$(REGISTRY) version=$(VERSION)

push-feature-server-java-docker:
	docker push $(REGISTRY)/feature-server-java:$(VERSION)

build-feature-server-java-docker:
	docker build --build-arg VERSION=$(VERSION) -t $(REGISTRY)/feature-server-java:$(VERSION) -f java/infra/docker/feature-server/Dockerfile .
