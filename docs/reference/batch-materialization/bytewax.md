# Bytewax

## Description

The [Bytewax](https://bytewax.io) batch materialization engine provides an execution
engine for batch materializing operations (`materialize` and `materialize-incremental`).

### Guide

In order to use the Bytewax materialization engine, you will need a [Kubernetes](https://kubernetes.io/) cluster running version 1.22.10 or greater.

#### Kubernetes Authentication

The Bytewax materialization engine loads authentication and cluster information from the [kubeconfig file](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/). By default, kubectl looks for a file named `config` in the `$HOME/.kube directory`. You can specify other kubeconfig files by setting the `KUBECONFIG` environment variable.

#### Resource Authentication

Bytewax jobs can be configured to access [Kubernetes secrets](https://kubernetes.io/docs/concepts/configuration/secret/) as environment variables to access online and offline stores during job runs.

To configure secrets, first create them using `kubectl`:

``` shell
kubectl create secret generic -n bytewax aws-credentials --from-literal=aws-access-key-id='<access key id>' --from-literal=aws-secret-access-key='<secret access key>'
```

If your Docker registry requires authentication to store/pull containers, you can use this same approach to store your repository access credential and use when running the materialization engine.

Then configure them in the batch_engine section of `feature_store.yaml`:

``` yaml
batch_engine:
  type: bytewax
  namespace: bytewax
  env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: aws-credentials
          key: aws-access-key-id
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: aws-credentials
          key: aws-secret-access-key
  image_pull_secrets:
    - docker-repository-access-secret
```

#### Configuration

The Bytewax materialization engine is configured through the The `feature_store.yaml` configuration file:

``` yaml
batch_engine:
  type: bytewax
  namespace: bytewax
  image: bytewax/bytewax-feast:latest
  image_pull_secrets:
    - my_container_secret
  service_account_name: my-k8s-service-account
  include_security_context_capabilities: false
  annotations:
    # example annotation you might include if running on AWS EKS
    iam.amazonaws.com/role: arn:aws:iam::<account number>:role/MyBytewaxPlatformRole
  resources:
    limits:
      cpu: 1000m
      memory: 2048Mi
    requests:
      cpu: 500m
      memory: 1024Mi
```

**Notes:**

* The `namespace` configuration directive specifies which Kubernetes [namespace](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) jobs, services and configuration maps will be created in.
* The `image_pull_secrets` configuration directive specifies the pre-configured secret to use when pulling the image container from your registry.
* The `service_account_name` specifies which Kubernetes service account to run the job under.
* The `include_security_context_capabilities` flag indicates whether or not `"add": ["NET_BIND_SERVICE"]` and `"drop": ["ALL"]` are included in the job & pod security context capabilities.
* `annotations` allows you to include additional Kubernetes annotations to the job. This is particularly useful for IAM roles which grant the running pod access to cloud platform resources (for example).
* The `resources` configuration directive sets the standard Kubernetes [resource requests](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/) for the job containers to utilise when materializing data.

#### Building a custom Bytewax Docker image

The `image` configuration directive specifies which container image to use when running the materialization job. To create a custom image based on this container, run the following command:

``` shell
DOCKER_BUILDKIT=1 docker build . -f ./sdk/python/feast/infra/materialization/contrib/bytewax/Dockerfile -t <image tag>
```

Once that image is built and pushed to a registry, it can be specified as a part of the batch engine configuration:

``` shell
batch_engine:
  type: bytewax
  namespace: bytewax
  image: <image tag>
```

