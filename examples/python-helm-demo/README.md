
# Running Feast Python / Go Feature Server with Redis on Kubernetes 

For this tutorial, we set up Feast with Redis. 

We use the Feast CLI to register and materialize features, and then retrieving via a Feast Python feature server deployed in Kubernetes

## First, let's set up a Redis cluster
1.  Start minikube (`minikube start`)
2.  Use helm to install a default Redis cluster
    ```bash
    helm repo add bitnami https://charts.bitnami.com/bitnami 
    helm repo update 
    helm install my-redis bitnami/redis
    ```
    ![](redis-screenshot.png)
3. Port forward Redis so we can materialize features to it
    
    ```bash
    kubectl port-forward --namespace default svc/my-redis-master 6379:6379
    ```
4. Get your Redis password using the command (pasted below for convenience). We'll need this to tell Feast how to communicate with the cluster.

   ```bash
    export REDIS_PASSWORD=$(kubectl get secret --namespace default my-redis -o jsonpath="{.data.redis-password}" | base64 --decode)
    echo $REDIS_PASSWORD
    ```

## Next, we setup a local Feast repo
1. Install Feast with Redis dependencies `pip install "feast[redis]"`
2. Make a bucket in GCS (or S3)
3. The feature repo is already setup here, so you just need to swap in your GCS bucket and Redis credentials.
    We need to modify the `feature_store.yaml`, which has two fields for you to replace:
    ```yaml
    registry: gs://[YOUR GCS BUCKET]/demo-repo/registry.db
    project: feast_python_demo
    provider: gcp
    online_store:
      type: redis
      # Note: this would normally be using instance URL's to access Redis
      connection_string: localhost:6379,password=[YOUR PASSWORD]
    offline_store:
      type: file
    entity_key_serialization_version: 2
    ```
4. Run `feast apply` from within the `feature_repo` directory to apply your local features to the remote registry
   - Note: you may need to authenticate to gcloud first with `gcloud auth login`
5. Materialize features to the online store:
    ```bash
    CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")                                    
    feast materialize-incremental $CURRENT_TIME
    ``` 

## Now let's setup the Feast Server
1. Add the gcp-auth addon to mount GCP credentials:
    ```bash
   minikube addons enable gcp-auth
   ```
2. Add Feast's Python/Go feature server chart repo
    ```bash
    helm repo add feast-charts https://feast-helm-charts.storage.googleapis.com
    helm repo update
    ```
3. For this tutorial, because we don't have a direct hosted endpoint into Redis, we need to change `feature_store.yaml` to talk to the Kubernetes Redis service 
   ```bash 
   sed -i '' 's/localhost:6379/my-redis-master:6379/g' feature_store.yaml
   ``` 
4. Install the Feast helm chart: `helm install feast-release feast-charts/feast-feature-server --set feature_store_yaml_base64=$(base64 feature_store.yaml)`
   > **Dev instructions**: if you're changing the java logic or chart, you can do 
   1. `eval $(minikube docker-env)`
   2. `make build-feature-server-dev`
   3. `helm install feast-release ../../../infra/charts/feast-feature-server --set image.tag=dev --set feature_store_yaml_base64=$(base64 feature_store.yaml)`
5. (Optional): check logs of the server to make sure it’s working
   ```bash
   kubectl logs svc/feast-feature-server
   ```
6. Port forward to expose the grpc endpoint:
   ```bash
   kubectl port-forward svc/feast-feature-server 6566:80
   ```
7. Run test fetches for online features:8. 
    - First: change back the Redis connection string to allow localhost connections to Redis
      ```bash
      sed -i '' 's/my-redis-master:6379/localhost:6379/g' feature_store.yaml
      ```
    - Then run the included fetch script, which fetches both via the HTTP endpoint and for comparison, via the Python SDK
      ```bash
        python test_python_fetch.py
      ```