feast-jupyter
=============
Feast Jupyter provides a Jupyter server with pre-installed Feast SDK

Current chart version is `0.6-SNAPSHOT`





## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| gcpServiceAccount.enabled | bool | `false` | Flag to use [service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) JSON key |
| gcpServiceAccount.existingSecret.key | string | `"credentials.json"` | Key in the secret data (file name of the service account) |
| gcpServiceAccount.existingSecret.name | string | `"feast-gcp-service-account"` | Name of the existing secret containing the service account |
| image.pullPolicy | string | `"Always"` | Image pull policy |
| image.repository | string | `"gcr.io/kf-feast/feast-jupyter"` | Docker image repository |
| image.tag | string | `"latest"` | Image tag |
| replicaCount | int | `1` | Number of pods that will be created |
