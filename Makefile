VERSION_FILE=VERSION
FEAST_VERSION=`cat $(VERSION_FILE)`

test:
	echo testing not implemented

build-deps:
	$(MAKE) -C protos gen-go
	dep ensure

build-cli:
	$(MAKE) build-deps
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