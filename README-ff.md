Additional README information only applicable to Farfetch.

### How-to: Build locally

This project runs on CI for all commits (on all branches) and creates artifacts based on the commit hash.
Using Cloud Build locally is generally not however, it can be useful if debugging the `cloudbuild.yaml`.

Install the required dependencies: https://cloud.google.com/cloud-build/docs/build-debug-locally

```bash
cloud-build-local --config=cloudbuild.yaml --dryrun=false --substitutions SHORT_SHA=$(git rev-parse --short HEAD) .
```

### How-to: Build single step locally

Some steps can be build directly on your own machine - follow the commands that `cloudbuild.yaml` is invoking.

If you do not have the required dependencies, you can translate the `cloudbuild.yaml` step into a Docker command. E.g.:

```bash
CLOUDBUILD_STEP="docker run --rm --name feast-build \
                     -v $(pwd):/workspace \
                     -v go_cache:/cache/go \
                     -v m2_cache:/cache/m2 \
                     -v pip_cache:/root/.cache/pip \
                     -e GO111MODULE=on \
                     -e GOPATH=/cache/go \
                     -e FEAST_VERSION=ff-$(git rev-parse --short HEAD)-dev \
                     -e MAVEN_OPTS=-Dmaven.repo.local=/cache/m2 \
                     -e GOOGLE_APPLICATION_CREDENTIALS=/etc/service-account/service-account.json
                     -e GOOGLE_CLOUD_PROJECT=dev-konnekt-data-deep-1 \
"
```

#### compile-protos-go
```bash
$CLOUDBUILD_STEP \
    -w /workspace \
    --entrypoint make \
    gcr.io/konnekt-core/protoc-go:3.6.1 \
    compile-protos-go
```

#### unit-test-java
```bash
$CLOUDBUILD_STEP \
    -w /workspace \
    --entrypoint mvn \
    maven:3.6.2-jdk-11-slim \
    -Drevision=ff-$(git rev-parse --short HEAD) \
    test
```

#### build-java
```bash
$CLOUDBUILD_STEP \
    -w /workspace \
    --entrypoint mvn \
    maven:3.6.2-jdk-11-slim \
    -Drevision=ff-$(git rev-parse --short HEAD) \
    -DskipTests=true \
    --batch-mode \
    package
```

#### gen-proto-python
```bash
$CLOUDBUILD_STEP \
    -w /workspace/protos \
    --entrypoint make \
    gcr.io/konnekt-core/protoc-python@sha256:61421f32abe11acac6a9aa6356a1b3cf009daa0fc3feb3d875e098fde422f8b0 \
   'gen-python'
```

#### unit-test-python-sdk
```bash
$CLOUDBUILD_STEP \
    -w /workspace/sdk/python \
    -v /Volumes/GoogleDrive/My\ Drive/credentials/dev-konnekt-data-deep-1_feast-dev.json:/etc/service-account/service-account.json \
    --entrypoint sh \
    python:3.7-buster \
    -c 'pip install -r requirements-ci.txt && pip install -e . && pytest --junitxml=/log/python-sdk-test-report.xml'
```

### How-to: Run locally

```bash
mvn clean -Drevision=dev -DskipTests=true --batch-mode package
docker-compose up --build
```

### How-to: Run e2e locally
```bash
mvn clean -Drevision=ff-$(git rev-parse --short HEAD)-dev -DskipTests=true --batch-mode package
docker run -it --name feast-e2e \
    -v $(pwd):/workspace \
    -v /Volumes/GoogleDrive/My\ Drive/credentials/dev-konnekt-data-deep-1_feast-dev.json:/etc/service-account/service-account.json \
    -w /workspace \
    -e SKIP_BUILD_JARS=true \
    -e GOOGLE_CLOUD_PROJECT=dev-konnekt-data-deep-1 \
    -e TEMP_BUCKET=dev-konnekt-data-deep-1-feast-tmp \
    -e JOBS_STAGING_LOCATION=gs://dev-konnekt-data-deep-1-feast-tmp/e2e-staging \
    -e JAR_VERSION_SUFFIX=$(git rev-parse --short HEAD) \
    maven:3.6.2-jdk-11 \
    bash
    infra/scripts/test-end-to-end-batch.sh
```

### How-to: Run locally in IDE

```bash
docker run --rm --name postgresql \
    -e POSTGRESQL_USERNAME=postgres \
    -e POSTGRESQL_PASSWORD=password \
    -e POSTGRESQL_DATABASE=postgres \
    -p 5432:5432 \
    bitnami/postgresql:11.5.0-debian-9-r84
```

```text
LOG_TYPE=JSON;PROJECT_ID=dev-konnekt-data-deep-1;TRAINING_DATASET_PREFIX=feast_training;JOB_RUNNER=DataflowRunner;JOB_WORKSPACE=gs://dev-konnekt-data-deep-1-dataflow-workspace;JOB_OPTIONS={};STORE_SERVING_TYPE=noop;STORE_SERVING_OPTIONS={};STORE_WAREHOUSE_TYPE=bigquery;STORE_WAREHOUSE_OPTIONS={"project": "dev-konnekt-data-deep-1", "dataset": "feast_warehouse"};STORE_ERRORS_TYPE=stdout;STORE_ERRORS_OPTIONS=;DATAFLOW_PROJECT_ID=dev-konnekt-data-deep-1;DATAFLOW_LOCATION=europe-west4
```

### Forward ports to run locally

```bash
kubectl port-forward -n deep svc/feast-postgresql-postgresql 5432:5432
kubectl port-forward -n deep svc/feast-redis-jobstore-headless 6379:6379
kubectl port-forward -n deep svc/feast-feast-core 6565:6565
kubectl port-forward -n deep svc/feast-feast-serving-batch 6566:6566
```

### Read sample messages from Kafka

```bash
kafka-console-consumer --bootstrap-server 10.163.12.6:9092 --topic feast-features --from-beginning --group test-iain --max-messages 10
```


### Common issues

This is a list of common issues experienced while getting familiar with the framework! If you discover other issues or
find that the solution did not solve your problem feel free to contribute with your findings.

* Issue: Connection timeout (feast core/serving) issue when running the e2e batch test via Docker
  Solution: In most of the cases we found that the issue was caused by the jars not being in the folder and causing
            the services not to start. Running `mvn clean -Drevision=ff-$(git rev-parse --short HEAD)-dev -DskipTests=true --batch-mode package` before should fix the issue.
* Issue: no manifest found in jars. This issue might be caused by other intermediate steps and their cached result conflict with other java builds. 
         The only solution for it so far has been re-cloning the repository.
* Issue: the e2e are not running from a direct pytest command after running `docker-compose up -d` inside `infra/docker-compose`because of a connection timeout. 
         It seems that sometimes postgres doesn't start properly, so it's worth checking if it's up and if it is restart feast core.
* Issue: e2e test `test_get_batch_features_with_file` fails occasionally caused by a `NoneType` . This issue is potentially linked to an input/fixture issue and is usually resolved by rerunning the e2e tests.

