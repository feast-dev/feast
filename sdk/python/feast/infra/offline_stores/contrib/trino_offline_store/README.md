# Trino offline store
This is a walkthrough to talk about how we can test the trino plugin locally

## Start Trino in a docker container
```sh
make start-trino-locally
```

Normally this should start a docker container named `trino` listening on the port 8080.
You can see the docker command executed by looking at the `Makefile` at the root.

You can look at the queries being executed during the tests with the [local cluster UI](http://0.0.0.0:8080/ui/#) running.
This can be helpful to debug the Trino plugin while executing tests.

## Run the universal suites locally
```sh
make test-trino-plugin-locally
```

## Kill the local Trino container
```sh
make kill-trino-locally
```

You can always look at the running containers and kill the ones you don't need anymore
```sh
docker ps
docker stop {NAME/SHA OF THE CONTAINER}
```
feast.sdk.python.feast.infra.offline_stores.contrib.trino_offline_store