.PHONY: go

build-deps:
	$(MAKE) -C protos gen-go
	dep ensure

build-cli:
	$(MAKE) -C cli build-all

install-cli:
	@$(MAKE) build-deps
	cd cli/feast && go install

build-docker:
	docker build -t $(registry)/feast-core:$(version) -f docker/core/Dockerfile .
	docker build -t $(registry)/feast-serving:$(version) -f docker/serving/Dockerfile .

build-push-docker:
	@$(MAKE) build-docker registry=$(registry) version=$(version)
	docker push $(registry)/feast-core:$(version)
	docker push $(registry)/feast-serving:$(version)
	