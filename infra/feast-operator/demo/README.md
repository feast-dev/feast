# Installing the sample notebook
Follow this procedure to install and configure a sample Jupyter Notebook in your Kubernetes environment that can
connect a deployed `FeatureStore` instance.

## Requirement
* `FeatureStore` instance deployed using the Feast Operator.
* Local environment logged to the target Kubernetes cluster with `admin` privileges.
* `kubectl` CLI
* A Notebook instance created in the Kubernetes cluster (either with KubeFlow, OpedDataHub or RedHat OpenShift AI)
  * No specific image is needed for the purpose, assuming that it includes the Python interpreter.

## Mounting the Feast resources
**Important Notes**: 
* This procedure causes a restart of the running notebook to apply the mounted resources. Save your changes, before
proceeding.
* This procedure updates the `Notebook` custom resource and must be executed just once. Next executions will break
the consistency of the manifest. In this case, edit the resource manifest and remove the duplicated settings.

Launch this command to mount the Feast client ConfigMap and the required TLS secrets to the only Notebook in the namespace:
```bash
./init_feast_notebook.sh 
```

In case you have multiple Notebooks or Feast instances, provide their names using the optional parameters:
```bash
% ./init_feast_notebook.sh --help
Usage: init_feast_notebook.sh [--notebook <notebook_name>] [--feast <feast_name>] [--help]

Options:
  --notebook   Specify the notebook name. Defaults to the only Notebook instance in the current namespace.
  --feast      Specify the Feast instance name. Defaults to the only Feast instance in the current namespace.
  --help       Display this help message.
```

Output example:
```console
Notebook Name: feast-client
Feast Name: example
ConfigMap Mount Path: /feast/feature_repository
Connecting Notebook 'feast-client' with Feast 'example'...
Patching Notebook feast-client with ConfigMap feast-example-client
notebook.kubeflow.org/feast-client patched
Offline TLS Mount Path: 
Online TLS Mount Path: /tls/online
Registry TLS Mount Path: /tls/registry
Patching Notebook feast-client with Secret feast-example-online-tls mounted at /tls/online
notebook.kubeflow.org/feast-client patched
Patching Notebook feast-client with Secret feast-example-registry-tls mounted at /tls/registry
notebook.kubeflow.org/feast-client patched
```

## Validating the Notebook

Wait until the notebook is ready (replace NOTEBOOK_NAME with your notebook name):
```bash
kubectl wait --for=condition=ready pod/NOTEBOOK_NAME-0 --timeout=2m
```

Then verify the content of the mounted folders (update `/feast` and `/tls` with your actual paths, if needed)
```bash
kubectl exec pod/NOTEBOOK_NAME-0 -c NOTEBOOK_NAME -- bash -c "find /feast; find /tls"
```

Output example:
```console
/feast
/feast/feature_repository
/feast/feature_repository/..2024_12_05_09_26_57.3976272882
/feast/feature_repository/..2024_12_05_09_26_57.3976272882/feature_store.yaml
/feast/feature_repository/..data
/feast/feature_repository/feature_store.yaml
/tls
/tls/online
/tls/online/tls.key
/tls/online/tls.crt
/tls/online/..data
/tls/online/..2024_12_05_09_26_57.1747038009
/tls/online/..2024_12_05_09_26_57.1747038009/tls.key
/tls/online/..2024_12_05_09_26_57.1747038009/tls.crt
/tls/registry
/tls/registry/tls.key
/tls/registry/tls.crt
/tls/registry/..data
/tls/registry/..2024_12_05_09_26_57.3730005170
/tls/registry/..2024_12_05_09_26_57.3730005170/tls.key
/tls/registry/..2024_12_05_09_26_57.3730005170/tls.crt
```

## Importing the quickstart Jupyter Notebook
The provided [quickstart](./quickstart.ipynb) notebook provides you a quick-start client application to connect the
deployed Feast instance:
* Install `feast` from either `git` (for latest developments) or `pypi` (for released versions).
* Connect to the deplpoyed Feast instance and validate the content:
  * Using `feast` CLI.
  * With `feast` python package.

Copy the notebook in your workbench, using either `git clone`, `kubectl cp` or `wget`/`curl` and enjoy the experience!
