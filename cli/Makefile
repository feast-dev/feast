.PHONY: go

build-all: 
	@$(MAKE) cli-linux cli-darwin

.PHONY: cli-linux
cli-linux: 
	mkdir -p bin
	export GOOS=linux; \
	export GOARCH=amd64; \
	mkdir -p bin/$$GOOS-$$GOARCH && go build -o bin/$$GOOS-$$GOARCH/feast feast/main.go

.PHONY: cli-darwin
cli-darwin:
	export GOOS=darwin; \
	export GOARCH=amd64; \
	mkdir -p bin/$$GOOS-$$GOARCH && go build -o bin/$$GOOS-$$GOARCH/feast feast/main.go