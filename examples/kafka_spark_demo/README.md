# Stream Feature View Demo with Kafka and Spark

This demo is based heavily on the feast spark kafka demo [here](https://github.com/feast-dev/feast-workshop/tree/main/module_1).

The Kafka docker container takes data from the feature repo and takes data(specifically the "miles_driven" column) and streams it as a kafka producer as fresher features.

The stream feature view will register this kafka topic and stream the fresher features and update the online store.

## Step 1: Install Feast

First, we install Feast with Spark and Redis support:
```bash
pip install "feast[spark,redis]"
```

## Step 2: Spin up Kafka + Redis + Feast services

We then use Docker Compose to spin up the services we need.
- This leverages a script (in `kafka_demo/`) that creates a topic, reads from `feature_repo/data/driver_stats.parquet`, generates newer timestamps, and emits them to the topic.
- This also deploys an instance of Redis.
- This also deploys a Feast push server (on port 6567) + a Feast feature server (on port 6566).
  - These servers embed a `feature_store.yaml` file that enables them to connect to a remote registry. The Dockerfile mostly delegates to calling the `feast serve` CLI command, which instantiates a Feast python server ([docs](https://docs.feast.dev/reference/feature-servers/python-feature-server)):
    ```yaml
    FROM python:3.7

    RUN pip install "feast[redis]"

    COPY feature_repo/feature_store.yaml feature_store.yaml

    # Needed to reach online store within Docker network.
    RUN sed -i 's/localhost:6379/redis:6379/g' feature_store.yaml
    ENV FEAST_USAGE=False

    CMD ["feast", "serve", "-h", "0.0.0.0"]
    ```

Start up the Docker daemon and then use Docker Compose to spin up the services as described above:
- You may need to run `sudo docker-compose up` if you run into a Docker permission denied error
```console
$ docker-compose up

## Step 3: Take a look at the streaming api demo in the "stream_transform_demo.ipynb".


