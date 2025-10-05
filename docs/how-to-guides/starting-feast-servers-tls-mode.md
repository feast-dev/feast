# Starting feast servers in TLS (SSL) mode.
TLS (Transport Layer Security) and SSL (Secure Sockets Layer) are both protocols encrypts communications between a client and server to provide enhanced security.TLS or SSL words used interchangeably. 
This article is going to show the sample code to start all the feast servers such as online server, offline server, registry server and UI server in TLS mode. 
Also show examples related to feast clients to communicate with the feast servers started in TLS mode. 

We assume you have basic understanding of feast terminology before going through this tutorial, if you are new to feast then we would recommend to go through existing [starter tutorials](./../../examples) of feast.

## Obtaining a self-signed TLS certificate and key
In development mode we can generate a self-signed certificate for testing. In an actual production environment it is always recommended to get it from a trusted TLS certificate provider.

```shell
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes
```

The above command will generate two files
* `key.pem` : certificate private key
* `cert.pem`: certificate public key

You can use the public or private keys generated from above command in the rest of the sections in this tutorial.

## Create the feast demo repo for the rest of the sections.
Create a feast repo and initialize using `feast init` and `feast apply` command and use this repo as a demo for subsequent sections.

```shell
feast init feast_repo_ssl_demo

#output will be something similar as below
Creating a new Feast repository in /Documents/Src/feast/feast_repo_ssl_demo.

cd feast_repo_ssl_demo/feature_repo
feast apply

#output will be something similar as below
Applying changes for project feast_repo_ssl_demo

Created project feast_repo_ssl_demo
Created entity driver
Created feature view driver_hourly_stats
Created feature view driver_hourly_stats_fresh
Created on demand feature view transformed_conv_rate
Created on demand feature view transformed_conv_rate_fresh
Created feature service driver_activity_v1
Created feature service driver_activity_v3
Created feature service driver_activity_v2

Created sqlite table feast_repo_ssl_demo_driver_hourly_stats_fresh
Created sqlite table feast_repo_ssl_demo_driver_hourly_stats
```

You need to execute the feast cli commands from  `feast_repo_ssl_demo/feature_repo` directory created from the above `feast init` command.

## Starting feast online server (feature server) in TLS mode
To start the feature server in TLS mode, you need to provide the private and public keys using the `--key` and `--cert` arguments with the `feast serve` command.

```shell
feast serve --key /path/to/key.pem --cert /path/to/cert.pem
```
You will see the output something similar to as below. Note the server url starts in the `https` mode.

```shell
[2024-11-04 15:03:57 -0500] [77989] [INFO] Starting gunicorn 23.0.0
[2024-11-04 15:03:57 -0500] [77989] [INFO] Listening at: https://127.0.0.1:6566 (77989)
[2024-11-04 15:03:57 -0500] [77989] [INFO] Using worker: uvicorn_worker.UvicornWorker
[2024-11-04 15:03:57 -0500] [77992] [INFO] Booting worker with pid: 77992
[2024-11-04 15:03:57 -0500] [77992] [INFO] Started server process [77992]
[2024-11-04 15:03:57 -0500] [77992] [INFO] Waiting for application startup.
[2024-11-04 15:03:57 -0500] [77992] [INFO] Application startup complete.
```


### Feast client connecting to remote online sever started in TLS mode.

Sometimes you may need to pass the self-signed public key to connect to the remote online server started in SSL mode if you have not added the public key to the certificate store.

feast client example:
The registry is pointing to registry of remote feature store. If it is not accessible then should be configured to use remote registry.

```yaml
project: feast-project
registry: /remote/data/registry.db
provider: local
online_store:
  path: http://localhost:6566
  type: remote
  cert: /path/to/cert.pem
entity_key_serialization_version: 3
auth:
  type: no_auth
```


`cert` is an optional configuration to the public certificate path when the online server starts in TLS(SSL) mode. Typically, this file ends with `*.crt`, `*.cer`, or `*.pem`.

## Starting feast Registry server in TLS mode
To start the feature server in TLS mode, you need to provide the private and public keys using the `--key` and `--cert` arguments with the `feast serve_registry` command.

```shell
feast serve_registry --key /path/to/key.pem --cert /path/to/cert.pem
```
You will see the output something similar to as below. Note the server url starts in the `https` mode.

```shell
11/04/2024 03:10:27 PM feast.registry_server INFO: Starting grpc registry server in TLS(SSL) mode
11/04/2024 03:10:27 PM feast.registry_server INFO: Grpc server started at https://localhost:6570
```

### Feast client connecting to remote registry sever started in TLS mode.

Sometimes you may need to pass the self-signed public key to connect to the remote registry server started in SSL mode if you have not added the public key to the certificate store.

feast client example:

```yaml
project: feast-project
registry:
  registry_type: remote
  path: https://localhost:6570
  cert: /path/to/cert.pem
provider: local
online_store:
  path: http://localhost:6566
  type: remote
  cert: /path/to/cert.pem
entity_key_serialization_version: 3
auth:
  type: no_auth
```

`cert` is an optional configuration to the public certificate path when the registry server starts in TLS(SSL) mode. Typically, this file ends with `*.crt`, `*.cer`, or `*.pem`.

## Starting feast offline server in TLS mode

To start the offline server in TLS mode, you need to provide the private and public keys using the `--key` and `--cert` arguments with the `feast serve_offline` command.

```shell
feast serve_offline --key /path/to/key.pem --cert /path/to/cert.pem
```
You will see the output something similar to as below. Note the server url starts in the `https` mode.

```shell
11/07/2024 11:10:01 AM feast.offline_server INFO: Found SSL certificates in the args so going to start offline server in TLS(SSL) mode.
11/07/2024 11:10:01 AM feast.offline_server INFO: Offline store server serving at: grpc+tls://127.0.0.1:8815
11/07/2024 11:10:01 AM feast.offline_server INFO: offline server starting with pid: [11606]
```

### Feast client connecting to remote offline sever started in TLS mode.

Sometimes you may need to pass the self-signed public key to connect to the remote registry server started in SSL mode if you have not added the public key to the certificate store.
You have to add `scheme` to `https`.

feast client example:

```yaml
project: feast-project
registry:
  registry_type: remote
  path: https://localhost:6570
  cert: /path/to/cert.pem
provider: local
online_store:
  path: http://localhost:6566
  type: remote
  cert: /path/to/cert.pem
entity_key_serialization_version: 3
offline_store:
  type: remote
  host: localhost
  port: 8815
  scheme: https
  cert: /path/to/cert.pem
auth:
  type: no_auth
```

`cert` is an optional configuration to the public certificate path when the registry server starts in TLS(SSL) mode. Typically, this file ends with `*.crt`, `*.cer`, or `*.pem`.
`scheme` should be `https`. By default, it will be `http` so you have to explicitly configure to `https` if you are planning to connect to remote offline server which is started in TLS mode.

## Starting feast UI server (react app) in TLS mode
To start the feast UI server in TLS mode, you need to provide the private and public keys using the `--key` and `--cert` arguments with the `feast ui` command.

```shell
feast ui --key /path/to/key.pem --cert /path/to/cert.pem
```
You will see the output something similar to as below. Note the server url starts in the `https` mode.

```shell
INFO:     Started server process [78872]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on https://0.0.0.0:8888 (Press CTRL+C to quit)
```


## Adding public key to CA trust store and configuring the feast to use the trust store.
You can pass the public key for SSL verification using the `cert` parameter, however, it is sometimes difficult to maintain individual certificates and pass them individually.
The alternative recommendation is to add the public certificate to CA trust store and set the path as an environment variable (e.g., `FEAST_CA_CERT_FILE_PATH`). Feast will use the trust store path in the  `FEAST_CA_CERT_FILE_PATH` environment variable.