feast-jupyter
=============
Feast Jupyter provides a Jupyter server with pre-installed Feast SDK

Current chart version is `0.6-SNAPSHOT`





## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| image.pullPolicy | string | `"Always"` | Image pull policy |
| image.repository | string | `"gcr.io/kf-feast/feast-jupyter"` | Docker image repository |
| image.tag | string | `"latest"` | Image tag |
| replicaCount | int | `1` | Number of pods that will be created |
