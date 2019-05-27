# Feast CLI

The feast command-line tool, `feast`, is used to register resources to
feast, as well as manage and run ingestion jobs.

## Installation

The quickest way to get the CLI is to download the compiled binary: 

```sh
# For Mac OS users
wget https://github.com/gojek/feast/releases/download/v0.1.1/feast-cli-v0.1.1-darwin-amd64
chmod +x feast-cli-v0.1.1-darwin-amd64
sudo mv feast-cli-v0.1.1-darwin-amd64 /usr/local/bin/

# For Linux users
wget https://github.com/gojek/feast/releases/download/v0.1.1/feast-cli-v0.1.1-linux-amd64
chmod +x feast-cli-v0.1.1-linux-amd64
sudo mv feast-cli-v0.1.1-linux-amd64 /usr/local/bin/
```

### Building from source

If you want to develop the CLI or build it from source, you need to have at least Golang version 1.11 installed because Feast use go modules.

```sh
git clone https://github.com/gojek/feast
cd feast
go build -o feast ./cli/feast

# Test running feast CLI
./feast
```
