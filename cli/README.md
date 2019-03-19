# Feast CLI

The feast command-line tool, `feast`, is used to register resources to
feast, as well as manage and run ingestion jobs.

## Installation

### Download the compiled binary

The quickest way to get the CLI is to download the compiled binary: #TODO

### Building from source

The following dependencies are required to build the CLI from source:
* [`go`](https://golang.org/)
* [`protoc`](https://developers.google.com/protocol-buffers/)
* [`dep`](https://github.com/golang/dep)

See below for specific instructions on how to install the dependencies.

After the dependencies are installed, you can build the CLI using:
```sh
# at feast top-level directory
$ make build-cli
```

### Dependencies

#### `protoc-gen-go`

To ensure you have a matching version of `protoc-gen-go`, install the vendored version:
```sh
$ go install  ./vendor/github.com/golang/protobuf/protoc-gen-go
$ which protoc-gen-go
~/go/bin/protoc-gen-go
```

#### `dep`

On MacOS you can install or upgrade to the latest released version with Homebrew:
```sh
$ brew install dep
$ brew upgrade dep
```

On other platforms you can use the `install.sh` script:
```sh
$ curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
```
