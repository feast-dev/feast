# Troubleshooting

If at any point in time you cannot resolve a problem, please see the [Getting Help](https://github.com/gojek/feast/tree/75f3b783e5a7c5e0217a3020422548fb0d0ce0bf/docs/getting-help.md) section for reaching out to the Feast community.

## How can I verify that all services are operational?

### Docker Compose

The containers should be in an `up` state:

```text
docker ps
```

```text
CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS                                            NAMES
d7447205bced        jupyter/datascience-notebook:latest    "tini -g -- start-no…"   2 minutes ago       Up 2 minutes        0.0.0.0:8888->8888/tcp                           feast_jupyter_1
8e49dbe81b92        gcr.io/kf-feast/feast-serving:latest   "java -Xms1024m -Xmx…"   2 minutes ago       Up 5 seconds        0.0.0.0:6567->6567/tcp                           feast_batch-serving_1
b859494bd33a        gcr.io/kf-feast/feast-serving:latest   "java -jar /opt/feas…"   2 minutes ago       Up About a minute   0.0.0.0:6566->6566/tcp                           feast_online-serving_1
5c4962811767        gcr.io/kf-feast/feast-core:latest      "java -jar /opt/feas…"   2 minutes ago       Up 2 minutes        0.0.0.0:6565->6565/tcp                           feast_core_1
1ba7239e0ae0        confluentinc/cp-kafka:5.2.1            "/etc/confluent/dock…"   2 minutes ago       Up 2 minutes        0.0.0.0:9092->9092/tcp, 0.0.0.0:9094->9094/tcp   feast_kafka_1
e2779672735c        confluentinc/cp-zookeeper:5.2.1        "/etc/confluent/dock…"   2 minutes ago       Up 2 minutes        2181/tcp, 2888/tcp, 3888/tcp                     feast_zookeeper_1
39ac26f5c709        postgres:12-alpine                     "docker-entrypoint.s…"   2 minutes ago       Up 2 minutes        5432/tcp                                         feast_db_1
3c4ee8616096        redis:5-alpine                         "docker-entrypoint.s…"   2 minutes ago       Up 2 minutes        0.0.0.0:6379->6379/tcp                           feast_redis_1
```

### Google Kubernetes Engine

All services should either be in a `running` state or `complete`state:

```text
kubectl get pods
```

```text
NAME                                                READY   STATUS      RESTARTS   AGE
feast-feast-core-5ff566f946-4wlbh                   1/1     Running     1          32m
feast-feast-serving-batch-848d74587b-96hq6          1/1     Running     2          32m
feast-feast-serving-online-df69755d5-fml8v          1/1     Running     2          32m
feast-kafka-0                                       1/1     Running     1          32m
feast-kafka-1                                       1/1     Running     0          30m
feast-kafka-2                                       1/1     Running     0          29m
feast-kafka-config-3e860262-zkzr8                   0/1     Completed   0          32m
feast-postgresql-0                                  1/1     Running     0          32m
feast-prometheus-statsd-exporter-554db85b8d-r4hb8   1/1     Running     0          32m
feast-redis-master-0                                1/1     Running     0          32m
feast-zookeeper-0                                   1/1     Running     0          32m
feast-zookeeper-1                                   1/1     Running     0          32m
feast-zookeeper-2                                   1/1     Running     0          31m
```

## How can I verify that I can connect to all services?

First find the `IP:Port` combination of your services.

### **Docker Compose \(from inside the docker cluster\)**

You will probably need to connect using the hostnames of services and standard Feast ports:

```bash
export FEAST_CORE_URL=core:6565
export FEAST_ONLINE_SERVING_URL=online-serving:6566
export FEAST_BATCH_SERVING_URL=batch-serving:6567
```

### **Docker Compose \(from outside the docker cluster\)**

You will probably need to connect using `localhost` and standard ports:

```bash
export FEAST_CORE_URL=localhost:6565
export FEAST_ONLINE_SERVING_URL=localhost:6566
export FEAST_BATCH_SERVING_URL=localhost:6567
```

### **Google Kubernetes Engine \(GKE\)**

You will need to find the external IP of one of the nodes as well as the NodePorts. Please make sure that your firewall is open for these ports:

```bash
export FEAST_IP=$(kubectl describe nodes | grep ExternalIP | awk '{print $2}' | head -n 1)
export FEAST_CORE_URL=${FEAST_IP}:32090
export FEAST_ONLINE_SERVING_URL=${FEAST_IP}:32091
export FEAST_BATCH_SERVING_URL=${FEAST_IP}:32092
```

`netcat`, `telnet`, or even `curl` can be used to test whether all services are available and ports are open, but `grpc_cli` is the most powerful. It can be installed from [here](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md).

### Testing Feast Core:

```bash
grpc_cli ls ${FEAST_CORE_URL} feast.core.CoreService
```

```text
GetFeastCoreVersion
GetFeatureSet
ListFeatureSets
ListStores
ApplyFeatureSet
UpdateStore
CreateProject
ArchiveProject
ListProjects
```

### Testing Feast Batch Serving and Online Serving

```bash
grpc_cli ls ${FEAST_BATCH_SERVING_URL} feast.serving.ServingService
```

```text
GetFeastServingInfo
GetOnlineFeatures
GetBatchFeatures
GetJob
```

```bash
grpc_cli ls ${FEAST_ONLINE_SERVING_URL} feast.serving.ServingService
```

```text
GetFeastServingInfo
GetOnlineFeatures
GetBatchFeatures
GetJob
```

## How can I print logs from the Feast Services?

Feast will typically have three services that you need to monitor if something goes wrong.

* Feast Core
* Feast Serving \(Online\)
* Feast Serving \(Batch\)

In order to print the logs from these services, please run the commands below.

### Docker Compose

```text
 docker logs -f feast_core_1
```

```text
docker logs -f feast_batch-serving_1
```

```text
docker logs -f feast_online-serving_1
```

### Google Kubernetes Engine

```text
kubectl logs $(kubectl get pods | grep feast-core | awk '{print $1}')
```

```text
kubectl logs $(kubectl get pods | grep feast-serving-batch | awk '{print $1}')
```

```text
kubectl logs $(kubectl get pods | grep feast-serving-online | awk '{print $1}')
```

