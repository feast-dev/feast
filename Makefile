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

VERSION_FILE                = VERSION
FEAST_VERSION               = `cat $(VERSION_FILE)`

test:
	mvn test

test-integration:
	$(MAKE) -C testing/integration test-integration TYPE=$(TYPE) ID=$(ID)

build-proto:
	$(MAKE) -C protos gen-go

build-cli:
	$(MAKE) build-proto
	$(MAKE) -C cli build-all

build-java:
	mvn clean verify -Drevision=$(FEAST_VERSION)

build-docker:
	docker build -t $(registry)/feast-core:$(version) -f docker/core/Dockerfile .
	docker build -t $(registry)/feast-serving:$(version) -f docker/serving/Dockerfile .

build-push-docker:
	@$(MAKE) build-docker registry=$(registry) version=$(version)
	docker push $(registry)/feast-core:$(version)
	docker push $(registry)/feast-serving:$(version)