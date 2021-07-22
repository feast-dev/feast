# Troubleshooting

{% hint style="warning" %}
This page applies to Feast 0.7. The content may be out of date for Feast 0.8+
{% endhint %}

If at any point in time you cannot resolve a problem, please see the [Community](../../community.md) section for reaching out to the Feast community.

### How can I verify that all services are operational?

#### Docker Compose

The containers should be in an `up` state:

```text
docker ps
```

#### Google Kubernetes Engine

All services should either be in a `RUNNING` state or `COMPLETED`state:

```text
kubectl get pods
```

### How can I verify that I can connect to all services?

First locate the the host and port of the Feast Services.

#### **Docker Compose \(from inside the docker network\)**

You will probably need to connect using the hostnames of services and standard Feast ports:

```bash
export FEAST_CORE_URL=core:6565
export FEAST_ONLINE_SERVING_URL=online_serving:6566
export FEAST_HISTORICAL_SERVING_URL=historical_serving:6567
export FEAST_JOBCONTROLLER_URL=jobcontroller:6570
```

#### **Docker Compose \(from outside the docker network\)**

You will probably need to connect using `localhost` and standard ports:

```bash
export FEAST_CORE_URL=localhost:6565
export FEAST_ONLINE_SERVING_URL=localhost:6566
export FEAST_HISTORICAL_SERVING_URL=localhost:6567
export FEAST_JOBCONTROLLER_URL=localhost:6570
```

#### **Google Kubernetes Engine \(GKE\)**

You will need to find the external IP of one of the nodes as well as the NodePorts. Please make sure that your firewall is open for these ports:

```bash
export FEAST_IP=$(kubectl describe nodes | grep ExternalIP | awk '{print $2}' | head -n 1)
export FEAST_CORE_URL=${FEAST_IP}:32090
export FEAST_ONLINE_SERVING_URL=${FEAST_IP}:32091
export FEAST_HISTORICAL_SERVING_URL=${FEAST_IP}:32092
```

`netcat`, `telnet`, or even `curl` can be used to test whether all services are available and ports are open, but `grpc_cli` is the most powerful. It can be installed from [here](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md).

#### Testing Connectivity From Feast Services:

Use `grpc_cli` to test connetivity by listing the gRPC methods exposed by Feast services:

```bash
grpc_cli ls ${FEAST_CORE_URL} feast.core.CoreService
```

```bash
grpc_cli ls ${FEAST_JOBCONTROLLER_URL} feast.core.JobControllerService
```

```bash
grpc_cli ls ${FEAST_HISTORICAL_SERVING_URL} feast.serving.ServingService
```

```bash
grpc_cli ls ${FEAST_ONLINE_SERVING_URL} feast.serving.ServingService
```

### How can I print logs from the Feast Services?

Feast will typically have three services that you need to monitor if something goes wrong.

* Feast Core
* Feast Job Controller
* Feast Serving \(Online\)
* Feast Serving \(Batch\)

In order to print the logs from these services, please run the commands below.

#### Docker Compose

Use `docker-compose logs` to obtain Feast component logs:

```text
 docker logs -f feast_core_1
```

```text
 docker logs -f feast_jobcontroller_1
```

```text
docker logs -f feast_historical_serving_1
```

```text
docker logs -f feast_online_serving_1
```

#### Google Kubernetes Engine

Use `kubectl logs` to obtain Feast component logs:

```text
kubectl logs $(kubectl get pods | grep feast-core | awk '{print $1}')
```

```text
kubectl logs $(kubectl get pods | grep feast-jobcontroller | awk '{print $1}')
```

```text
kubectl logs $(kubectl get pods | grep feast-serving-batch | awk '{print $1}')
```

```text
kubectl logs $(kubectl get pods | grep feast-serving-online | awk '{print $1}')
```

