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

ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# install tools in project (tool) dir to not pollute the system
TOOL_DIR := $(ROOT_DIR)/tools
export GOBIN=$(TOOL_DIR)/bin
export PATH := $(TOOL_DIR)/bin:$(PATH)

MVN := mvn -f java/pom.xml ${MAVEN_EXTRA_OPTS}
OS := linux
ifeq ($(shell uname -s), Darwin)
	OS = osx
endif
TRINO_VERSION ?= 376
PYTHON_VERSION = ${shell python --version | grep -Eo '[0-9]\.[0-9]+'}

PYTHON_VERSIONS := 3.10 3.11 3.12

define get_env_name
$(subst .,,py$(1))
endef


# General
$(TOOL_DIR):
	mkdir -p $@/bin


help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

format: format-python ## Format Python code

lint: lint-python ## Lint Python code

test: test-python-unit ## Run unit tests for Python

protos: compile-protos-python compile-protos-docs ## Compile protobufs for Python SDK and generate docs

build: protos build-docker ## Build protobufs and Docker images

format-python: ## Format Python code
	uv run ruff check --fix sdk/python/feast/ sdk/python/tests/
	uv run ruff format sdk/python/feast/ sdk/python/tests/

# File-aware format (for use with pre-commit, accepts file args)
format-python-files: ## Format specified Python files
	@if [ -n "$(FILES)" ]; then \
		uv run ruff check --fix $(FILES) && \
		uv run ruff format $(FILES); \
	else \
		echo "Usage: make format-python-files FILES='file1.py file2.py'"; \
	fi


lint-python: ## Lint Python code
	uv run ruff check sdk/python/feast/ sdk/python/tests/
	uv run ruff format --check sdk/python/feast/ sdk/python/tests/
	uv run bash -c "cd sdk/python && mypy feast"

# File-aware lint (for use with pre-commit, accepts file args)
lint-python-files: ## Lint specified Python files
	@if [ -n "$(FILES)" ]; then \
		uv run ruff check $(FILES) && \
		uv run ruff format --check $(FILES); \
	else \
		echo "Usage: make lint-python-files FILES='file1.py file2.py'"; \
	fi


# New combined target
precommit-check: format-python lint-python ## Run all precommit checks
	@echo "✅ All precommit checks passed"

# Install precommit hooks with correct stages
install-precommit: ## Install precommit hooks (runs on commit, not push)
	pip install pre-commit
	pre-commit install --hook-type pre-commit
	@echo "✅ Precommit hooks installed (will run on commit, not push)"

# Manual full type check
mypy-full: ## Full MyPy type checking with all files
	uv run bash -c "cd sdk/python && mypy feast tests"

# Run precommit on all files
precommit-all: ## Run all precommit hooks on all files
	pre-commit run --all-files

# Make scripts executable
setup-scripts: ## Make helper scripts executable
	chmod +x scripts/uv-run.sh scripts/check-init-py.sh scripts/mypy-daemon.sh

##@ Python SDK - local
# formerly install-python-ci-dependencies-uv-venv
# editable install
install-python-dependencies-dev: ## Install Python dev dependencies using uv (editable install)
	uv pip sync --require-hashes sdk/python/requirements/py$(PYTHON_VERSION)-ci-requirements.txt
	uv pip install --no-deps -e .

install-python-dependencies-minimal: ## Install minimal Python dependencies using uv (editable install)
	uv pip sync --require-hashes sdk/python/requirements/py$(PYTHON_VERSION)-minimal-requirements.txt
	uv pip install --no-deps -e .[minimal]

##@ Python SDK - CI (uses uv with virtualenv)
# Uses uv pip sync with virtualenv for CI environments

# Used in github actions/ci
install-python-dependencies-ci: ## Install Python CI dependencies using uv pip sync
	# Create virtualenv if it doesn't exist
	uv venv .venv
	# Install CPU-only torch first to prevent CUDA dependency issues (Linux only)
	@if [ "$$(uname -s)" = "Linux" ]; then \
		echo "Installing dependencies with torch CPU index for Linux..."; \
		uv pip sync --extra-index-url https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match sdk/python/requirements/py$(PYTHON_VERSION)-ci-requirements.txt; \
	else \
		echo "Installing dependencies from PyPI for macOS..."; \
		uv pip sync sdk/python/requirements/py$(PYTHON_VERSION)-ci-requirements.txt; \
	fi
	uv pip install --no-deps -e .

# Used in github actions/ci
install-hadoop-dependencies-ci: ## Install Hadoop dependencies
	@if [ ! -f $$HOME/hadoop-3.4.2.tar.gz ]; then \
		echo "Downloading Hadoop tarball..."; \
		wget -q https://dlcdn.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz -O $$HOME/hadoop-3.4.2.tar.gz; \
	else \
		echo "Using cached Hadoop tarball"; \
	fi
	@if [ ! -d $$HOME/hadoop ]; then \
		echo "Extracting Hadoop tarball..."; \
		tar -xzf $$HOME/hadoop-3.4.2.tar.gz -C $$HOME; \
		mv $$HOME/hadoop-3.4.2 $$HOME/hadoop; \
	fi
install-python-ci-dependencies: ## Install Python CI dependencies in system environment using piptools
	python -m piptools sync sdk/python/requirements/py$(PYTHON_VERSION)-ci-requirements.txt
	pip install --no-deps -e .

# Currently used in test-end-to-end.sh
install-python: ## Install Python requirements and develop package (setup.py develop)
	python -m piptools sync sdk/python/requirements/py$(PYTHON_VERSION)-requirements.txt
	python setup.py develop

lock-python-dependencies-all: ## Recompile and lock all Python dependency sets for all supported versions
	# Remove all existing requirements because we noticed the lock file is not always updated correctly.
	# Removing and running the command again ensures that the lock file is always up to date.
	rm -rf sdk/python/requirements/* 2>/dev/null || true
	$(foreach ver,$(PYTHON_VERSIONS),\
		pixi run --environment $(call get_env_name,$(ver)) --manifest-path infra/scripts/pixi/pixi.toml \
			"uv pip compile -p $(ver) --no-strip-extras setup.py --extra ci \
			--generate-hashes --output-file sdk/python/requirements/py$(ver)-ci-requirements.txt" && \
		pixi run --environment $(call get_env_name,$(ver)) --manifest-path infra/scripts/pixi/pixi.toml \
			"uv pip compile -p $(ver) --no-strip-extras setup.py \
			--generate-hashes --output-file sdk/python/requirements/py$(ver)-requirements.txt" && \
		pixi run --environment $(call get_env_name,$(ver)) --manifest-path infra/scripts/pixi/pixi.toml \
			"uv pip compile -p $(ver) --no-strip-extras setup.py --extra minimal \
			--generate-hashes --output-file sdk/python/requirements/py$(ver)-minimal-requirements.txt" && \
		pixi run --environment $(call get_env_name,$(ver)) --manifest-path infra/scripts/pixi/pixi.toml \
			"uv pip compile -p $(ver) --no-strip-extras setup.py --extra minimal-sdist-build \
			--no-emit-package milvus-lite \
			--generate-hashes --output-file sdk/python/requirements/py$(ver)-minimal-sdist-requirements.txt" && \
		pixi run --environment $(call get_env_name,$(ver)) --manifest-path infra/scripts/pixi/pixi.toml \
			"uv pip install -p $(ver) pybuild-deps==0.5.0 pip==25.0.1 && \
			pybuild-deps compile --generate-hashes \
			-o sdk/python/requirements/py$(ver)-minimal-sdist-requirements-build.txt \
			sdk/python/requirements/py$(ver)-minimal-sdist-requirements.txt" && \
	) true

compile-protos-python: ## Compile Python protobuf files
	python infra/scripts/generate_protos.py

benchmark-python: ## Run integration + benchmark tests for Python
	IS_TEST=True python -m pytest --integration --benchmark  --benchmark-autosave --benchmark-save-data sdk/python/tests

benchmark-python-local: ## Run integration + benchmark tests for Python (local dev mode)
	IS_TEST=True FEAST_IS_LOCAL_TEST=True python -m pytest --integration --benchmark  --benchmark-autosave --benchmark-save-data sdk/python/tests

##@ Tests

test-python-unit: ## Run Python unit tests (use pattern=<pattern> to filter tests, e.g., pattern=milvus, pattern=test_online_retrieval.py, pattern=test_online_retrieval.py::test_get_online_features_milvus)
	uv run python -m pytest -n 8 --color=yes $(if $(pattern),-k "$(pattern)") sdk/python/tests

# Fast unit tests only
test-python-unit-fast: ## Run fast unit tests only (no external dependencies)
	uv run python -m pytest sdk/python/tests/unit -n auto -x --tb=short

# Changed files only (requires pytest-testmon)
test-python-changed: ## Run tests for changed files only
	uv run python -m pytest --testmon -n 8 --tb=short sdk/python/tests

# Quick smoke test for PRs
test-python-smoke: ## Quick smoke test for development
	uv run python -m pytest \
		sdk/python/tests/unit/test_unit_feature_store.py \
		sdk/python/tests/unit/test_repo_operations_validate_feast_project_name.py \
		-n 4 --tb=short

test-python-integration: ## Run Python integration tests (CI)
	uv run python -m pytest --tb=short -v -n 8 --integration --color=yes --durations=10 --timeout=1200 --timeout_method=thread --dist loadgroup \
		-k "(not snowflake or not test_historical_features_main)" \
		-m "not rbac_remote_integration_test" \
		--log-cli-level=INFO -s \
		sdk/python/tests

# Integration tests with better parallelization
test-python-integration-parallel: ## Run integration tests with enhanced parallelization
	uv run python -m pytest sdk/python/tests/integration \
		-n auto --dist loadscope \
		--timeout=300 --tb=short -v \
		--integration --color=yes --durations=20

test-python-integration-local: ## Run Python integration tests (local dev mode)
	FEAST_IS_LOCAL_TEST=True \
	FEAST_LOCAL_ONLINE_CONTAINER=True \
	HADOOP_HOME=$$HOME/hadoop \
	CLASSPATH="$$( $$HADOOP_HOME/bin/hadoop classpath --glob ):$$CLASSPATH" \
	HADOOP_USER_NAME=root \
	uv run python -m pytest --tb=short -v -n auto --color=yes --integration --durations=10 --timeout=1200 --timeout_method=thread --dist loadgroup \
		-k "not test_lambda_materialization and not test_snowflake_materialization" \
		-m "not rbac_remote_integration_test" \
		--log-cli-level=INFO -s \
		sdk/python/tests

test-python-integration-rbac-remote: ## Run Python remote RBAC integration tests
	FEAST_IS_LOCAL_TEST=True \
	FEAST_LOCAL_ONLINE_CONTAINER=True \
	uv run python -m pytest --tb=short -v -n 8 --color=yes --integration --durations=10 --timeout=1200 --timeout_method=thread --dist loadgroup \
		-k "not test_lambda_materialization and not test_snowflake_materialization" \
		-m "rbac_remote_integration_test" \
		--log-cli-level=INFO -s \
		sdk/python/tests

test-python-integration-container: ## Run Python integration tests using Docker
	@(docker info > /dev/null 2>&1 && \
		FEAST_LOCAL_ONLINE_CONTAINER=True \
		uv run python -m pytest -n 8 --integration sdk/python/tests \
	) || echo "This script uses Docker, and it isn't running - please start the Docker Daemon and try again!";

test-python-integration-dbt: ## Run dbt integration tests
	@echo "Running dbt integration tests..."
	@cd sdk/python/tests/integration/dbt/test_dbt_project && \
		echo "Installing dbt dependencies..." && \
		dbt deps && \
		echo "Building dbt models..." && \
		dbt build
	@cd sdk/python/tests/integration/dbt && \
		echo "Setting up Feast project..." && \
		mkdir -p feast_repo/data && \
		echo "project: feast_dbt_test\nregistry: data/registry.db\nprovider: local\nonline_store:\n  type: sqlite\n  path: data/online_store.db" > feast_repo/feature_store.yaml
	@cd sdk/python/tests/integration/dbt/feast_repo && \
		echo "Testing feast dbt import..." && \
		feast dbt import -m ../test_dbt_project/target/manifest.json -e driver_id -d file --tag feast && \
		echo "Verifying Feast objects..." && \
		feast feature-views list && \
		feast entities list
	@cd sdk/python && \
		echo "Running pytest integration tests..." && \
		python -m pytest tests/integration/dbt/test_dbt_integration.py -v --tb=short
	@echo "✓ dbt integration tests completed successfully!"

test-python-universal-spark: ## Run Python Spark integration tests
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

test-python-historical-retrieval:
	## Run Python historical retrieval integration tests
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.spark_repo_configuration \
	PYTEST_PLUGINS=feast.infra.offline_stores.contrib.spark_offline_store.tests \
 	python -m pytest -n 8 --integration \
 	 	-k "test_historical_retrieval_with_validation or \
			test_historical_features_persisting or \
			test_historical_retrieval_fails_on_validation" \
 	 sdk/python/tests

test-python-universal-trino: ## Run Python Trino integration tests
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
test-python-universal-mssql: ## Run Python MSSQL integration tests
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
test-python-universal-athena: ## Run Python Athena integration tests
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

test-python-universal-postgres-offline: ## Run Python Postgres integration tests
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
 				not test_spark" \
 			sdk/python/tests

 test-python-universal-clickhouse-offline: ## Run Python Clickhouse integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.clickhouse_repo_configuration \
		PYTEST_PLUGINS=sdk.python.feast.infra.offline_stores.contrib.clickhouse_offline_store.tests \
		python -m pytest -v -n 8 --integration \
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
 				not test_spark" \
 			sdk/python/tests

test-python-universal-ray-offline: ## Run Python Ray offline store integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.ray_repo_configuration \
		PYTEST_PLUGINS=sdk.python.feast.infra.offline_stores.contrib.ray_offline_store.tests \
		python -m pytest -n 8 --integration \
			-m "not universal_online_stores and not benchmark" \
			-k "not test_historical_retrieval_with_validation and \
				not test_universal_cli and \
				not test_go_feature_server and \
				not test_feature_logging and \
				not test_logged_features_validation and \
				not test_lambda_materialization_consistency and \
				not gcs_registry and \
				not s3_registry and \
				not test_snowflake and \
				not test_spark" \
			sdk/python/tests

test-python-ray-compute-engine: ## Run Python Ray compute engine tests
	PYTHONPATH='.' \
		python -m pytest -v --integration \
			sdk/python/tests/integration/compute_engines/ray_compute/

test-python-universal-postgres-online: ## Run Python Postgres integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.postgres_online_store.postgres_repo_configuration \
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

 test-python-universal-pgvector-online: ## Run Python Postgres integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.postgres_online_store.pgvector_repo_configuration \
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
 				not test_validation and \
 				not test_spark_materialization_consistency and \
 				not test_historical_features_containing_backfills and \
				not test_snowflake" \
 			sdk/python/tests

test-python-universal-mysql-online: ## Run Python MySQL integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.mysql_online_store.mysql_repo_configuration \
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

test-python-universal-cassandra: ## Run Python Cassandra integration tests
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.cassandra_online_store.cassandra_repo_configuration \
	PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.cassandra \
	python -m pytest -x --integration \
	sdk/python/tests/integration/offline_store/test_feature_logging.py \
		--ignore=sdk/python/tests/integration/offline_store/test_validation.py \
		-k "not test_snowflake and \
			not test_spark_materialization_consistency and \
			not test_universal_materialization"

test-python-universal-hazelcast: ## Run Python Hazelcast integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.hazelcast_online_store.hazelcast_repo_configuration \
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

test-python-universal-cassandra-no-cloud-providers: ## Run Python Cassandra integration tests
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.cassandra_online_store.cassandra_repo_configuration \
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

test-python-universal-elasticsearch-online: ## Run Python Elasticsearch online store integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.elasticsearch_online_store.elasticsearch_repo_configuration \
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

test-python-universal-milvus-online: ## Run Python Milvus online store integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.milvus_online_store.milvus_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.milvus \
		python -m pytest -n 8 --integration \
		-k "test_retrieve_online_milvus_documents" \
 			sdk/python/tests --ignore=sdk/python/tests/integration/offline_store/test_dqm_validation.py

test-python-universal-singlestore-online: ## Run Python Singlestore online store integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.singlestore_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.singlestore \
		python -m pytest -n 8 --integration \
			-k "not test_universal_cli and \
				not gcs_registry and \
				not s3_registry and \
				not test_snowflake" \
			sdk/python/tests

test-python-universal-qdrant-online: ## Run Python Qdrant online store integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.qdrant_online_store.qdrant_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.qdrant \
		python -m pytest -n 8 --integration \
 			-k "test_retrieve_online_documents" \
 			sdk/python/tests/integration/online_store/test_universal_online.py

# To use Couchbase as an offline store, you need to create an Couchbase Capella Columnar cluster on cloud.couchbase.com.
# Modify environment variables COUCHBASE_COLUMNAR_CONNECTION_STRING, COUCHBASE_COLUMNAR_USER, and COUCHBASE_COLUMNAR_PASSWORD
# with the details from your Couchbase Columnar Cluster.
test-python-universal-couchbase-offline: ## Run Python Couchbase offline store integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.couchbase_columnar_repo_configuration \
		PYTEST_PLUGINS=feast.infra.offline_stores.contrib.couchbase_offline_store.tests \
		COUCHBASE_COLUMNAR_CONNECTION_STRING=couchbases://<connection_string> \
		COUCHBASE_COLUMNAR_USER=username \
		COUCHBASE_COLUMNAR_PASSWORD=password \
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

test-python-universal-couchbase-online:	## Run Python Couchbase online store integration tests
	PYTHONPATH='.' \
		FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.couchbase_online_store.couchbase_repo_configuration \
		PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.couchbase \
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

test-python-universal: ## Run all Python integration tests
	uv run python -m pytest -n 8 --integration sdk/python/tests

##@ Java

install-java-ci-dependencies: ## Install Java CI dependencies
	${MVN} verify clean --fail-never

format-java: ## Format Java code
	${MVN} spotless:apply

lint-java: ## Lint Java code
	${MVN} --no-transfer-progress spotless:check

test-java: ## Run Java unit tests
	${MVN} --no-transfer-progress -DskipITs=true test

test-java-integration: ## Run Java integration tests
	${MVN} --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -DskipUTs=true clean verify

test-java-with-coverage: ## Run Java unit tests with coverage
	${MVN} --no-transfer-progress -DskipITs=true test jacoco:report-aggregate

build-java: ## Build Java code
	${MVN} clean verify

build-java-no-tests: ## Build Java code without running tests
	${MVN} --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -DskipUTs=true -DskipITs=true -Drevision=${REVISION} clean package

##@ Trino plugin
start-trino-locally: ## Start Trino locally
	cd ${ROOT_DIR}; docker run --detach --rm -p 8080:8080 --name trino -v ${ROOT_DIR}/sdk/python/feast/infra/offline_stores/contrib/trino_offline_store/test_config/properties/:/etc/catalog/:ro trinodb/trino:${TRINO_VERSION}
	sleep 15

test-trino-plugin-locally: ## Run Trino plugin tests locally
	cd ${ROOT_DIR}/sdk/python; FULL_REPO_CONFIGS_MODULE=feast.infra.offline_stores.contrib.trino_offline_store.test_config.manual_tests IS_TEST=True python -m pytest --integration tests/

kill-trino-locally: ## Kill Trino locally
	cd ${ROOT_DIR}; docker stop trino

##@ Docker

build-docker: build-feature-server-docker build-feature-transformation-server-docker build-feature-server-java-docker build-feast-operator-docker ## Build Docker images

push-ci-docker: ## Push CI Docker images
	docker push $(REGISTRY)/feast-ci:$(VERSION)

push-feature-server-docker: ## Push Feature Server Docker image
	docker push $(REGISTRY)/feature-server:$(VERSION)

build-feature-server-docker: ## Build Feature Server Docker image
	docker buildx build \
		-t $(REGISTRY)/feature-server:$(VERSION) \
		-f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile \
		--load sdk/python/feast/infra/feature_servers/multicloud

push-feature-transformation-server-docker: ## Push Feature Transformation Server Docker image
	docker push $(REGISTRY)/feature-transformation-server:$(VERSION)

build-feature-transformation-server-docker: ## Build Feature Transformation Server Docker image
	docker buildx build --build-arg VERSION=$(VERSION) \
		-t $(REGISTRY)/feature-transformation-server:$(VERSION) \
		-f sdk/python/feast/infra/transformation_servers/Dockerfile --load .

push-feature-server-java-docker: ## Push Feature Server Java Docker image
	docker push $(REGISTRY)/feature-server-java:$(VERSION)

build-feature-server-java-docker: ## Build Feature Server Java Docker image
	docker buildx build --build-arg VERSION=$(VERSION) \
		-t $(REGISTRY)/feature-server-java:$(VERSION) \
		-f java/infra/docker/feature-server/Dockerfile --load .

push-feast-operator-docker: ## Push Feast Operator Docker image
	cd infra/feast-operator && \
	IMAGE_TAG_BASE=$(REGISTRY)/feast-operator \
	VERSION=$(VERSION) \
	$(MAKE) docker-push

build-feast-operator-docker: ## Build Feast Operator Docker image
	cd infra/feast-operator && \
	IMAGE_TAG_BASE=$(REGISTRY)/feast-operator \
	VERSION=$(VERSION) \
	$(MAKE) docker-build

build-feast-operator-docker-on-mac: ## Build Feast Operator Docker image on Mac
	cd infra/feast-operator && \
	IMAGE_TAG_BASE=$(REGISTRY)/feast-operator \
	VERSION=$(VERSION) \
	$(MAKE) docker-build-on-mac

build-go-feature-server-docker: ## Build Go Feature Server Docker iamge
	make compile-protos-go
	docker buildx build --build-arg VERSION=$(VERSION) \
		-t $(REGISTRY)/go-feature-server:$(VERSION) \
		-f go/infra/docker/feature-server/Dockerfile --load .

push-go-feature-server-docker: ## Push Go Feature Server Docker image
	docker push $(REGISTRY)/go-feature-server:$(VERSION)

##@ Dev images

build-feature-server-dev: ## Build Feature Server Dev Docker image
	docker buildx build \
		-t feastdev/feature-server:dev \
		-f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.dev --load .

build-feature-server-dev-docker: ## Build Feature Server Dev Docker image
	docker buildx build \
		-t $(REGISTRY)/feature-server:$(VERSION) \
		-f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.dev --load .

build-feature-server-dev-docker_on_mac: ## Build Feature Server Dev Docker image on Mac
	docker buildx build --platform linux/amd64 \
		-t $(REGISTRY)/feature-server:$(VERSION) \
		-f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile.dev --load .

push-feature-server-dev-docker: ## Push Feature Server Dev Docker image
	docker push $(REGISTRY)/feature-server:$(VERSION)

build-java-docker-dev: ## Build Java Dev Docker image
	make build-java-no-tests REVISION=dev
	docker buildx build --build-arg VERSION=dev \
		-t feastdev/feature-transformation-server:dev \
		-f sdk/python/feast/infra/transformation_servers/Dockerfile --load .
	docker buildx build --build-arg VERSION=dev \
		-t feastdev/feature-server-java:dev \
		-f java/infra/docker/feature-server/Dockerfile.dev --load .

##@ Documentation

install-dependencies-proto-docs: ## Install dependencies for generating proto docs
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

compile-protos-docs: ## Compile proto docs
	rm -rf 	$(ROOT_DIR)/dist/grpc
	mkdir -p dist/grpc;
	cd ${ROOT_DIR}/protos && protoc --docs_out=../dist/grpc feast/*/*.proto

build-sphinx: compile-protos-python ## Build Sphinx documentation
	cd 	$(ROOT_DIR)/sdk/python/docs && $(MAKE) build-api-source

build-templates: ## Build templates
	python infra/scripts/compile-templates.py

build-helm-docs: ## Build helm docs
	cd ${ROOT_DIR}/infra/charts/feast; helm-docs
	cd ${ROOT_DIR}/infra/charts/feast-feature-server; helm-docs

##@ Web UI
# Note: these require node and yarn to be installed

build-ui: ## Build Feast UI
	cd $(ROOT_DIR)/sdk/python/feast/ui && yarn upgrade @feast-dev/feast-ui --latest && yarn install && npm run build --omit=dev

build-ui-local: ## Build Feast UI locally
	cd $(ROOT_DIR)/ui && yarn install && npm run build --omit=dev
	rm -rf $(ROOT_DIR)/sdk/python/feast/ui/build
	cp -r $(ROOT_DIR)/ui/build $(ROOT_DIR)/sdk/python/feast/ui/

format-ui: ## Format Feast UI
	cd $(ROOT_DIR)/ui && NPM_TOKEN= yarn install && NPM_TOKEN= yarn format


##@ Go SDK
PB_REL = https://github.com/protocolbuffers/protobuf/releases
PB_VERSION = 30.2
PB_ARCH := $(shell uname -m)
ifeq ($(PB_ARCH), arm64)
	PB_ARCH=aarch_64
endif
ifeq ($(PB_ARCH), aarch64)
	PB_ARCH=aarch_64
endif
PB_PROTO_FOLDERS=core registry serving types storage

$(TOOL_DIR)/protoc-$(PB_VERSION)-$(OS)-$(PB_ARCH).zip: $(TOOL_DIR)
	cd $(TOOL_DIR) && \
	curl -LO $(PB_REL)/download/v$(PB_VERSION)/protoc-$(PB_VERSION)-$(OS)-$(PB_ARCH).zip

.PHONY: install-go-proto-dependencies
install-go-proto-dependencies: $(TOOL_DIR)/protoc-$(PB_VERSION)-$(OS)-$(PB_ARCH).zip ## Install Go proto dependencies
	unzip -u $(TOOL_DIR)/protoc-$(PB_VERSION)-$(OS)-$(PB_ARCH).zip -d $(TOOL_DIR)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

.PHONY: compile-protos-go
compile-protos-go: install-go-proto-dependencies ## Compile Go protobuf files
	mkdir -p $(ROOT_DIR)/go/protos
	$(foreach folder,$(PB_PROTO_FOLDERS), \
		protoc --proto_path=$(ROOT_DIR)/protos \
			--go_out=$(ROOT_DIR)/go/protos \
			--go_opt=module=github.com/feast-dev/feast/go/protos \
			--go-grpc_out=$(ROOT_DIR)/go/protos \
			--go-grpc_opt=module=github.com/feast-dev/feast/go/protos $(ROOT_DIR)/protos/feast/$(folder)/*.proto; ) true

install-go-ci-dependencies:
	go install golang.org/x/tools/cmd/goimports
	uv pip install "pybindgen==0.22.1" "grpcio-tools>=1.56.2,<2" "mypy-protobuf>=3.1"

.PHONY: build-go
build-go: compile-protos-go ## Build Go code
	go build -o feast ./go/main.go


## Assume the uv will create an .venv folder for itself.
## The unit test funcions will call the Python "feast" command to initialze a feast repo.
.PHONY: install-feast-locally
install-feast-locally: ## Install Feast locally
	uv pip install -e "."
	@export PATH=$(ROOT_DIR)/.venv/bin:$$PATH
	@echo $$PATH

.PHONY: test-go
test-go: compile-protos-python compile-protos-go install-go-ci-dependencies install-feast-locally  ## Run Go tests
	CGO_ENABLED=1 go test -coverprofile=coverage.out ./... && go tool cover -html=coverage.out -o coverage.html

.PHONY: format-go
format-go: ## Format Go code
	gofmt -s -w go/

.PHONY: lint-go
lint-go: compile-protos-go ## Lint Go code
	go vet ./go/internal/feast

.PHONY: build-go-docker-dev
build-go-docker-dev: ## Build Go Docker image for development
	docker buildx build --build-arg VERSION=dev \
		-t feastdev/feature-server-go:dev \
		-f go/infra/docker/feature-server/Dockerfile --load .
