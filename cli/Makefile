.PHONY: go

build-all:
	@$(MAKE) cli-linux cli-darwin

.PHONY: cli-linux
cli-linux:
	mkdir -p bin
	export GOOS=linux; \
	export GOARCH=amd64; \
	go build \
	-ldflags "-X main.Version=${FEAST_VERSION}" \
	-o bin/feast.$$GOOS-$$GOARCH feast/main.go

.PHONY: cli-darwin
cli-darwin:
	mkdir -p bin
	export GOOS=darwin; \
	export GOARCH=amd64; \
	go build \
	-ldflags "-X main.Version=${FEAST_VERSION}" \
	-o bin/feast.$$GOOS-$$GOARCH feast/main.go