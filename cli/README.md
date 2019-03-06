# Feast CLI

The feast command-line tool, `feast`, is used to register resources to feast, as well as manage and run ingestion jobs.

## Installation

### Download the compiled binary

The quickest way to get the CLI is to download the compiled binary: #TODO

### Building from source

The following dependencies are required to install the CLI from source:
#### protoc
To install protoc, find the compiled binary for your distribution [here](https://github.com/protocolbuffers/protobuf/releases), then install it:
```
PROTOC_VERSION=3.3.0

curl -OL https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/$PROTOC_ZIP
unzip -o $PROTOC_ZIP -d /usr/local bin/protoc
rm -f $PROTOC_ZIP
```

#### dep
On MacOS you can install or upgrade to the latest released version with Homebrew:


```
brew install dep
brew upgrade dep
```

On other platforms you can use the install.sh script:
```
curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
```

#### Building 

This repo need to be checked out in `~/go/src/github.com/gojek/feast`.

Then build the cli using:
```
# at feast top-level directory
make build-cli
```

The built binaries will then be available in:
```
cli/bin/darwin-amd64/feast
cli/bin/linux-amd64/feast
```
