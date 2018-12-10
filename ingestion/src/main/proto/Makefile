.PHONY: go

dirs = core jobs serving specs storage types

gen-go:
	@$(foreach dir,$(dirs),protoc -I/usr/local/include -I. --go_out=plugins=grpc:$$GOPATH/src feast/$(dir)/*.proto;)