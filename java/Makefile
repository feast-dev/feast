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
MVN := mvn ${MAVEN_EXTRA_OPTS}

# General

format: format-java

lint: lint-java

test: test-java

build: build-java build-docker

install-ci-dependencies: install-java-ci-dependencies

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

# Docker

build-push-docker:
	@$(MAKE) build-docker registry=$(REGISTRY) version=$(VERSION)
	@$(MAKE) push-core-docker registry=$(REGISTRY) version=$(VERSION)
	@$(MAKE) push-serving-docker registry=$(REGISTRY) version=$(VERSION)

build-docker: build-core-docker build-serving-docker

push-core-docker:
	docker push $(REGISTRY)/feast-core:$(VERSION)

push-serving-docker:
	docker push $(REGISTRY)/feast-serving:$(VERSION)

build-core-docker:
	docker build --build-arg VERSION=$(VERSION) -t $(REGISTRY)/feast-core:$(VERSION) -f infra/docker/core/Dockerfile .

build-serving-docker:
	docker build --build-arg VERSION=$(VERSION) -t $(REGISTRY)/feast-serving:$(VERSION) -f infra/docker/serving/Dockerfile .
