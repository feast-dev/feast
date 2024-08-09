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

format: format-python format-java

lint: lint-python lint-java

test: test-python-unit test-java

protos: compile-protos-python compile-protos-docs

build: protos build-java build-docker

# Python SDK

install-python-ci-dependencies:
	python -m piptools sync sdk/python/requirements/py$(PYTHON)-ci-requirements.txt
	pip install --no-deps -e .
	python setup.py build_python_protos --inplace

install-python-ci-dependencies-uv:
	uv pip sync --system sdk/python/requirements/py$(PYTHON)-ci-requirements.txt
	uv pip install --system --no-deps -e .
	python setup.py build_python_protos --inplace

install-python-ci-dependencies-uv-venv:
	uv pip sync sdk/python/requirements/py$(PYTHON)-ci-requirements.txt
	uv pip install --no-deps -e .
	python setup.py build_python_protos --inplace

lock-python-ci-dependencies:
	uv pip compile --system --no-strip-extras setup.py --extra ci --output-file sdk/python/requirements/py$(PYTHON)-ci-requirements.txt

package-protos:
	cp -r ${ROOT_DIR}/protos ${ROOT_DIR}/sdk/python/feast/protos

compile-protos-python:
	python setup.py build_python_protos --inplace

install-python:
	python -m piptools sync sdk/python/requirements/py$(PYTHON)-requirements.txt
	python setup.py develop

lock-python-dependencies:
	uv pip compile --system --no-strip-extras setup.py --output-file sdk/python/requirements/py$(PYTHON)-requirements.txt

lock-python-dependencies-all:
	pixi run --environment py39 --manifest-path infra/scripts/pixi/pixi.toml "uv pip compile --system --no-strip-extras setup.py --output-file sdk/python/requirements/py3.9-requirements.txt"
	pixi run --environment py39 --manifest-path infra/scripts/pixi/pixi.toml "uv pip compile --system --no-strip-extras setup.py --extra ci --output-file sdk/python/requirements/py3.9-ci-requirements.txt"
	pixi run --environment py310 --manifest-path infra/scripts/pixi/pixi.toml "uv pip compile --system --no-strip-extras setup.py --output-file sdk/python/requirements/py3.10-requirements.txt"
	pixi run --environment py310 --manifest-path infra/scripts/pixi/pixi.toml "uv pip compile --system --no-strip-extras setup.py --extra ci --output-file sdk/python/requirements/py3.10-ci-requirements.txt"
	pixi run --environment py311 --manifest-path infra/scripts/pixi/pixi.toml "uv pip compile --system --no-strip-extras setup.py --output-file sdk/python/requirements/py3.11-requirements.txt"
	pixi run --environment py311 --manifest-path infra/scripts/pixi/pixi.toml "uv pip compile --system --no-strip-extras setup.py --extra ci --output-file sdk/python/requirements/py3.11-ci-requirements.txt"

benchmark-python:
	IS_TEST=True python -m pytest --integration --benchmark  --benchmark-autosave --benchmark-save-data sdk/python/tests

benchmark-python-local:
	IS_TEST=True FEAST_IS_LOCAL_TEST=True python -m pytest --integration --benchmark  --benchmark-autosave --benchmark-save-data sdk/python/tests

test-python-unit:
	python -m pytest -n 8 --color=yes sdk/python/tests

test-python-integration:
	python -m pytest -n 8 --integration --color=yes --durations=10 --timeout=1200 --timeout_method=thread \
		-k "(not snowflake or not test_historical_features_main)" \
		sdk/python/tests

test-python-integration-local:
	FEAST_IS_LOCAL_TEST=True \
	FEAST_LOCAL_ONLINE_CONTAINER=True \
	python -m pytest -n 8 --color=yes --integration --durations=10 --timeout=1200 --timeout_method=thread --dist loadgroup \
		-k "not test_lambda_materialization and not test_snowflake_materialization" \
		sdk/python/tests

test-python-integration-container:
	@(docker info > /dev/null 2>&1 && \
		FEAST_LOCAL_ONLINE_CONTAINER=True \
		python -m pytest -n 8 --integration sdk/python/tests \
	) || echo "This script uses Docker, and it isn't running - please start the Docker Daemon and try again!";

test-python-universal-spark:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.spark_repo_configuration \
	PYTEST_PLUGINS=feast.infra.offline_stores.contrib.spark_offline_store.tests \
 	python -m pytest -n 8 --integration \
 	 	-k "not test_historical_retrieval_fails_on_validation and \
			not test_historical_retrieval_with_validation and \
			not test_historical_features_persisting and \
			not test_historical_retrieval_fails_on_validation and \
			not test_universal_cli and \
			not test_go_feature_server and \
			not test_feature_logging and \
			not test_reorder_columns and \
			not test_logged_features_validation and \
			not test_lambda_materialization_consistency and \
			not test_offline_write and \
			not test_push_features_to_offline_store.py and \
			not gcs_registry and \
			not s3_registry and \
			not test_universal_types and \
			not test_snowflake" \
 	 sdk/python/tests

test-python-universal-trino:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.trino_repo_configuration \
	PYTEST_PLUGINS=feast.infra.offline_stores.contrib.trino_offline_store.tests \
 	python -m pytest -n 8 --integration \
 	 	-k "not test_historical_retrieval_fails_on_validation and \
			not test_historical_retrieval_with_validation and \
			not test_historical_features_persisting and \
			not test_historical_retrieval_fails_on_validation and \
			not test_universal_cli and \
			not test_go_feature_server and \
			not test_feature_logging and \
			not test_reorder_columns and \
			not test_logged_features_validation and \
			not test_lambda_materialization_consistency and \
			not test_offline_write and \
			not test_push_features_to_offline_store.py and \
			not gcs_registry and \
			not s3_registry and \
			not test_universal_types and \
            not test_snowflake" \
 	 sdk/python/tests


# Note: to use this, you'll need to have Microsoft ODBC 17 installed.
# See https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver15#17
test-python-universal-mssql:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.mssql_repo_configuration \
	PYTEST_PLUGINS=feast.infra.offline_stores.contrib.mssql_offline_store.tests \
	FEAST_LOCAL_ONLINE_CONTAINER=True \
 	python -m pytest -n 8 --integration \
 	 	-k "not gcs_registry and \
			not s3_registry and \
			not test_lambda_materialization and \
			not test_snowflake and \
			not test_historical_features_persisting and \
			not validation and \
			not test_feature_service_logging" \
 	 sdk/python/tests


# To use Athena as an offline store, you need to create an Athena database and an S3 bucket on AWS.
# https://docs.aws.amazon.com/athena/latest/ug/getting-started.html
# Modify environment variables ATHENA_REGION, ATHENA_DATA_SOURCE, ATHENA_DATABASE, ATHENA_WORKGROUP or
# ATHENA_S3_BUCKET_NAME according to your needs. If tests fail with the pytest -n 8 option, change the number to 1.
test-python-universal-athena:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.athena_repo_configuration \
	PYTEST_PLUGINS=feast.infra.offline_stores.contrib.athena_offline_store.tests \
	ATHENA_REGION=ap-northeast-2 \
	ATHENA_DATA_SOURCE=AwsDataCatalog \
	ATHENA_DATABASE=default \
	ATHENA_WORKGROUP=primary \
	ATHENA_S3_BUCKET_NAME=feast-int-bucket \
 	python -m pytest -n 8 --integration \
 	 	-k "not test_go_feature_server and \
		    not test_logged_features_validation and \
		    not test_lambda and \
		    not test_feature_logging and \
		    not test_offline_write and \
		    not test_push_offline and \
		    not test_historical_retrieval_with_validation and \
		    not test_historical_features_persisting and \
		    not test_historical_retrieval_fails_on_validation and \
			not gcs_registry and \
			not s3_registry and \
			not test_snowflake" \
	sdk/python/tests

test-python-universal-postgres-offline:
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.postgres_repo_configuration \
		PYTEST_PLUGINS=sdk.python.feast.infra.offline_stores.contrib.postgres_offline_store.tests \
		python -m pytest -n 8 --integration \
 			-k "not test_historical_retrieval_with_validation and \
				not test_historical_features_persisting and \
 				not test_universal_cli and \
 				not test_go_feature_server and \
 				not test_feature_logging and \
				not test_reorder_columns and \
				not test_logged_features_validation and \
				not test_lambda_materialization_consistency and \
				not test_offline_write and \
				not test_push_features_to_offline_store and \
				not gcs_registry and \
				not s3_registry and \
				not test_snowflake and \
 				not test_universal_types" \
 			sdk/python/tests

test-python-universal-postgres-online:
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.contrib.postgres_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.postgres \
		python -m pytest -n 8 --integration \
 			-k "not test_universal_cli and \
 				not test_go_feature_server and \
 				not test_feature_logging and \
				not test_reorder_columns and \
				not test_logged_features_validation and \
				not test_lambda_materialization_consistency and \
				not test_offline_write and \
				not test_push_features_to_offline_store and \
				not gcs_registry and \
				not s3_registry and \
 				not test_universal_types and \
				not test_snowflake" \
 			sdk/python/tests

 test-python-universal-pgvector-online:
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.contrib.pgvector_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.postgres \
		python -m pytest -n 8 --integration \
 			-k "not test_universal_cli and \
 				not test_go_feature_server and \
 				not test_feature_logging and \
				not test_reorder_columns and \
				not test_logged_features_validation and \
				not test_lambda_materialization_consistency and \
				not test_offline_write and \
				not test_push_features_to_offline_store and \
				not gcs_registry and \
				not s3_registry and \
 				not test_universal_types and \
				not test_snowflake" \
 			sdk/python/tests

 test-python-universal-mysql-online:
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.contrib.mysql_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.mysql \
		python -m pytest -n 8 --integration \
 			-k "not test_universal_cli and \
 				not test_go_feature_server and \
 				not test_feature_logging and \
				not test_reorder_columns and \
				not test_logged_features_validation and \
				not test_lambda_materialization_consistency and \
				not test_offline_write and \
				not test_push_features_to_offline_store and \
				not gcs_registry and \
				not s3_registry and \
 				not test_universal_types and \
				not test_snowflake" \
 			sdk/python/tests

test-python-universal-cassandra:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.contrib.cassandra_repo_configuration \
	PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.cassandra \
	python -m pytest -x --integration \
	sdk/python/tests

test-python-universal-hazelcast:
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.contrib.hazelcast_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.hazelcast \
		python -m pytest -n 8 --integration \
 			-k "not test_universal_cli and \
 				not test_go_feature_server and \
 				not test_feature_logging and \
				not test_reorder_columns and \
				not test_logged_features_validation and \
				not test_lambda_materialization_consistency and \
				not test_offline_write and \
				not test_push_features_to_offline_store and \
				not gcs_registry and \
				not s3_registry and \
 				not test_universal_types and \
				not test_snowflake" \
 			sdk/python/tests

test-python-universal-cassandra-no-cloud-providers:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.contrib.cassandra_repo_configuration \
	PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.cassandra \
	python -m pytest -x --integration \
	-k "not test_lambda_materialization_consistency   and \
	  not test_apply_entity_integration               and \
	  not test_apply_feature_view_integration         and \
	  not test_apply_entity_integration               and \
	  not test_apply_feature_view_integration         and \
	  not test_apply_data_source_integration          and \
	  not test_nullable_online_store				  and \
	  not gcs_registry 								  and \
	  not s3_registry								  and \
	  not test_snowflake" \
	sdk/python/tests

 test-python-universal-elasticsearch-online:
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.contrib.elasticsearch_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.elasticsearch \
		python -m pytest -n 8 --integration \
 			-k "not test_universal_cli and \
 				not test_go_feature_server and \
 				not test_feature_logging and \
				not test_reorder_columns and \
				not test_logged_features_validation and \
				not test_lambda_materialization_consistency and \
				not test_offline_write and \
				not test_push_features_to_offline_store and \
				not gcs_registry and \
				not s3_registry and \
 				not test_universal_types and \
				not test_snowflake" \
 			sdk/python/tests

test-python-universal-singlestore-online:
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.contrib.singlestore_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.singlestore \
		python -m pytest -n 8 --integration \
			-k "not test_universal_cli and \
				not gcs_registry and \
				not s3_registry and \
				not test_snowflake" \
			sdk/python/tests

test-python-universal:
	python -m pytest -n 8 --integration sdk/python/tests

format-python:
	cd ${ROOT_DIR}/sdk/python; python -m ruff check --fix feast/ tests/
	cd ${ROOT_DIR}/sdk/python; python -m ruff format feast/ tests/

lint-python:
	cd ${ROOT_DIR}/sdk/python; python -m mypy feast
	cd ${ROOT_DIR}/sdk/python; python -m ruff check feast/ tests/
	cd ${ROOT_DIR}/sdk/python; python -m ruff format --check feast/ tests
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
	cd ${ROOT_DIR}/sdk/python; FULL_REPO_CONFIGS_MODULE=feast.infra.offline_stores.contrib.trino_offline_store.test_config.manual_tests IS_TEST=True python -m pytest --integration tests/

kill-trino-locally:
	cd ${ROOT_DIR}; docker stop trino

install-protoc-dependencies:
	pip install --ignore-installed protobuf==4.24.0 "grpcio-tools>=1.56.2,<2" mypy-protobuf==3.1.0

# Docker

build-docker: build-feature-server-python-aws-docker build-feature-transformation-server-docker build-feature-server-java-docker

push-ci-docker:
	docker push $(REGISTRY)/feast-ci:$(VERSION)

push-feature-server-docker:
	docker push $(REGISTRY)/feature-server:$$VERSION

build-feature-server-docker:
	docker buildx build --build-arg VERSION=$$VERSION \
		-t $(REGISTRY)/feature-server:$$VERSION \
		-f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile --load .

push-feature-transformation-server-docker:
	docker push $(REGISTRY)/feature-transformation-server:$(VERSION)

build-feature-transformation-server-docker:
	docker buildx build --build-arg VERSION=$(VERSION) \
		-t $(REGISTRY)/feature-transformation-server:$(VERSION) \
		-f sdk/python/feast/infra/transformation_servers/Dockerfile --load .

push-feature-server-java-docker:
	docker push $(REGISTRY)/feature-server-java:$(VERSION)

build-feature-server-java-docker:
	docker buildx build --build-arg VERSION=$(VERSION) \
		-t $(REGISTRY)/feature-server-java:$(VERSION) \
		-f java/infra/docker/feature-server/Dockerfile --load .

push-feast-operator-docker:
	cd infra/feast-operator && \
	IMAGE_TAG_BASE=$(REGISTRY)/feast-operator \
	VERSION=$(VERSION) \
	$(MAKE) docker-push

build-feast-operator-docker:
	cd infra/feast-operator && \
	IMAGE_TAG_BASE=$(REGISTRY)/feast-operator \
	VERSION=$(VERSION) \
	$(MAKE) docker-build

# Dev images

build-feature-server-dev:
	docker buildx build --build-arg VERSION=dev \
		-t feastdev/feature-server:dev \
		-f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.dev --load .

build-java-docker-dev:
	make build-java-no-tests REVISION=dev
	docker buildx build --build-arg VERSION=dev \
		-t feastdev/feature-transformation-server:dev \
		-f sdk/python/feast/infra/transformation_servers/Dockerfile --load .
	docker buildx build --build-arg VERSION=dev \
		-t feastdev/feature-server-java:dev \
		-f java/infra/docker/feature-server/Dockerfile.dev --load .

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

build-helm-docs:
	cd ${ROOT_DIR}/infra/charts/feast; helm-docs
	cd ${ROOT_DIR}/infra/charts/feast-feature-server; helm-docs

# Web UI

# Note: requires node and yarn to be installed
build-ui:
	cd $(ROOT_DIR)/sdk/python/feast/ui && yarn upgrade @feast-dev/feast-ui --latest && yarn install && npm run build --omit=dev
