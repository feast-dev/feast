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
PROTO_TYPE_SUBDIRS = core serving types storage third_party/grpc/connector
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
	cd sdk/python && python -m piptools sync requirements/py$(PYTHON)-ci-requirements.txt
	cd sdk/python && python setup.py develop

lock-python-ci-dependencies:
	cd sdk/python && python -m piptools compile -U --extra ci --output-file requirements/py$(PYTHON)-ci-requirements.txt

package-protos:
	cp -r ${ROOT_DIR}/protos ${ROOT_DIR}/sdk/python/feast/protos

compile-protos-python:
	@$(foreach dir,$(PROTO_TYPE_SUBDIRS),cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --grpc_python_out=../sdk/python/feast/protos/ --python_out=../sdk/python/feast/protos/ --mypy_out=../sdk/python/feast/protos/ feast/$(dir)/*.proto;)
	@$(foreach dir,$(PROTO_TYPE_SUBDIRS),grep -rli 'from feast.$(dir)' sdk/python/feast/protos | xargs -I@ sed -i.bak 's/from feast.$(dir)/from feast.protos.feast.$(dir)/g' @;)

install-python:
	cd sdk/python && python -m piptools sync requirements/py$(PYTHON)-requirements.txt
	cd sdk/python && python setup.py develop

lock-python-dependencies:
	cd sdk/python && python -m piptools compile -U --output-file requirements/py$(PYTHON)-requirements.txt

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

test-python-go-server:
	FEAST_USAGE=False IS_TEST=True python -m pytest -n 8 --integration --goserver sdk/python/tests

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
	mkdir -p ./go/protos
	$(foreach dir,$(PROTO_TYPE_SUBDIRS),cd ${ROOT_DIR}/protos; protoc -I/usr/local/include -I. --go-grpc_out=paths=source_relative:../go/protos --go_out=paths=source_relative:../go/protos feast/$(dir)/*.proto;)

test-go:
	go test ./...

format-go:
	gofmt -s -w go/**/*.go

lint-go:
	go vet ./go/feast ./go/server

# Docker

build-docker: build-ci-docker build-feature-server-python-aws-docker build-feature-transformation-server-docker build-feature-server-java-docker

push-ci-docker:
	docker push $(REGISTRY)/feast-ci:$(VERSION)

# TODO(adchia): consider removing. This doesn't run successfully right now
build-ci-docker:
	docker build -t $(REGISTRY)/feast-ci:$(VERSION) -f infra/docker/ci/Dockerfile .

push-feature-server-python-aws-docker:
		docker push $(REGISTRY)/feature-server-python-aws:$$VERSION

build-feature-server-python-aws-docker:
		docker build --build-arg VERSION=$$VERSION \
			-t $(REGISTRY)/feature-server-python-aws:$$VERSION \
			-f sdk/python/feast/infra/feature_servers/aws_lambda/Dockerfile .

push-feature-transformation-server-docker:
	docker push $(REGISTRY)/feature-transformation-server:$(VERSION)

build-feature-transformation-server-docker:
	docker build --build-arg VERSION=$(VERSION) \
		-t $(REGISTRY)/feature-transformation-server:$(VERSION) \
		-f sdk/python/feast/infra/transformation_servers/Dockerfile .

push-feature-server-java-docker:
	docker push $(REGISTRY)/feature-server-java:$(VERSION)

build-feature-server-java-docker:
	docker build --build-arg VERSION=$(VERSION) \
		-t $(REGISTRY)/feature-server-java:$(VERSION) \
		-f java/infra/docker/feature-server/Dockerfile .

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
