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
OS := linux
ifeq ($(shell uname -s), Darwin)
	OS = osx
endif
TRINO_VERSION ?= 376

# General

format: format-python format-java format-go

lint: lint-python lint-java lint-go

test: test-python test-java test-go

protos: compile-protos-go compile-protos-python compile-protos-docs

build: protos build-java build-docker

# Python SDK

install-python-ci-dependencies: install-go-proto-dependencies install-go-ci-dependencies
	python -m piptools sync sdk/python/requirements/py$(PYTHON)-ci-requirements.txt
	COMPILE_GO=true python setup.py develop

lock-python-ci-dependencies:
	python -m piptools compile -U --extra ci --output-file sdk/python/requirements/py$(PYTHON)-ci-requirements.txt

package-protos:
	cp -r ${ROOT_DIR}/protos ${ROOT_DIR}/sdk/python/feast/protos

compile-protos-python:
	python setup.py build_python_protos

install-python:
	python -m piptools sync sdk/python/requirements/py$(PYTHON)-requirements.txt
	python setup.py develop

lock-python-dependencies:
	python -m piptools compile -U --output-file sdk/python/requirements/py$(PYTHON)-requirements.txt

benchmark-python:
	FEAST_USAGE=False IS_TEST=True python -m pytest --integration --benchmark  --benchmark-autosave --benchmark-save-data sdk/python/tests

benchmark-python-local:
	FEAST_USAGE=False IS_TEST=True FEAST_IS_LOCAL_TEST=True python -m pytest --integration --benchmark  --benchmark-autosave --benchmark-save-data sdk/python/tests

test-python:
	FEAST_USAGE=False IS_TEST=True python -m pytest -n 8 sdk/python/tests

test-python-integration:
	FEAST_USAGE=False IS_TEST=True python -m pytest -n 8 --integration sdk/python/tests

test-python-integration-container:
	FEAST_USAGE=False IS_TEST=True FEAST_LOCAL_ONLINE_CONTAINER=True python -m pytest -n 8 --integration sdk/python/tests

test-python-universal-contrib:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.contrib_repo_configuration \
	PYTEST_PLUGINS=feast.infra.offline_stores.contrib.trino_offline_store.tests \
 	FEAST_USAGE=False IS_TEST=True \
 	python -m pytest -n 8 --integration --universal \
 	 	-k "not test_historical_retrieval_fails_on_validation and \
			not test_historical_retrieval_with_validation and \
			not test_historical_features_persisting and \
			not test_historical_retrieval_fails_on_validation and \
			not test_universal_cli and \
			not test_go_feature_server and \
			not test_feature_logging and \
			not test_universal_types" \
 	 sdk/python/tests

test-python-universal-postgres:
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.postgres_repo_configuration \
		PYTEST_PLUGINS=sdk.python.feast.infra.offline_stores.contrib.postgres_offline_store.tests \
		FEAST_USAGE=False \
		IS_TEST=True \
		python -m pytest -x --integration --universal \
			-k "not test_historical_retrieval_fails_on_validation and \
				not test_historical_retrieval_with_validation and \
				not test_historical_features_persisting and \
				not test_historical_retrieval_fails_on_validation and \
				not test_universal_cli and \
				not test_go_feature_server and \
				not test_feature_logging and \
				not test_universal_types" \
			sdk/python/tests

test-python-universal-local:
	FEAST_USAGE=False IS_TEST=True FEAST_IS_LOCAL_TEST=True python -m pytest -n 8 --integration --universal sdk/python/tests

test-python-universal:
	FEAST_USAGE=False IS_TEST=True python -m pytest -n 8 --integration --universal sdk/python/tests

test-python-go-server: compile-go-lib
	FEAST_USAGE=False IS_TEST=True FEAST_GO_FEATURE_RETRIEVAL=True pytest --integration --goserver sdk/python/tests

format-python:
	# Sort
	cd ${ROOT_DIR}/sdk/python; python -m isort feast/ tests/

	# Format
	cd ${ROOT_DIR}/sdk/python; python -m black --target-version py37 feast tests

lint-python:
	cd ${ROOT_DIR}/sdk/python; python -m mypy
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

# Trino plugin
start-trino-locally:
	cd ${ROOT_DIR}; docker run --detach --rm -p 8080:8080 --name trino -v ${ROOT_DIR}/sdk/python/feast/infra/offline_stores/contrib/trino_offline_store/test_config/properties/:/etc/catalog/:ro trinodb/trino:${TRINO_VERSION}
	sleep 15

test-trino-plugin-locally:
	cd ${ROOT_DIR}/sdk/python; FULL_REPO_CONFIGS_MODULE=feast.infra.offline_stores.contrib.trino_offline_store.test_config.manual_tests FEAST_USAGE=False IS_TEST=True python -m pytest --integration --universal tests/

kill-trino-locally:
	cd ${ROOT_DIR}; docker stop trino

# Go SDK & embedded

install-go-proto-dependencies:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.26.0
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0

install-go-ci-dependencies:
	# ToDo: currently gopy installation doesn't work w/o explicit go get in the next line
	# ToDo: there should be a better way to install gopy
	go get github.com/go-python/gopy
	go install golang.org/x/tools/cmd/goimports
	go install github.com/go-python/gopy
	python -m pip install pybindgen==0.22.0

install-protoc-dependencies:
	pip install grpcio-tools==1.44.0 mypy-protobuf==3.1.0

compile-protos-go: install-go-proto-dependencies install-protoc-dependencies
	python setup.py build_go_protos

compile-go-lib: install-go-proto-dependencies install-go-ci-dependencies
	COMPILE_GO=True python setup.py build_ext --inplace

# Needs feast package to setup the feature store
test-go: compile-protos-go
	pip install -e ".[ci]"
	go test ./...

format-go:
	gofmt -s -w go/

lint-go: compile-protos-go
	go vet ./go/internal/feast ./go/embedded

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
