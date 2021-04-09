feast-jobservice
================
Feast Job Service manage ingestion jobs.

Current chart version is `0.9.5`





## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| envOverrides | object | `{}` | Extra environment variables to set |
| gcpProjectId | string | `""` | Project ID to use when using Google Cloud services such as BigQuery, Cloud Storage and Dataflow |
| gcpServiceAccount.enabled | bool | `false` | Flag to use [service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) JSON key |
| gcpServiceAccount.existingSecret.key | string | `"credentials.json"` | Key in the secret data (file name of the service account) |
| gcpServiceAccount.existingSecret.name | string | `"feast-gcp-service-account"` | Name of the existing secret containing the service account |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.repository | string | `"feastdev/feast-jobservice"` | Docker image repository |
| image.tag | string | `"develop"` | Image tag |
| ingress.grpc.annotations | object | `{}` | Extra annotations for the ingress |
| ingress.grpc.auth.enabled | bool | `false` | Flag to enable auth |
| ingress.grpc.class | string | `"nginx"` | Which ingress controller to use |
| ingress.grpc.enabled | bool | `false` | Flag to create an ingress resource for the service |
| ingress.grpc.hosts | list | `[]` | List of hostnames to match when routing requests |
| ingress.grpc.https.enabled | bool | `true` | Flag to enable HTTPS |
| ingress.grpc.https.secretNames | object | `{}` | Map of hostname to TLS secret name |
| ingress.grpc.whitelist | string | `""` | Allowed client IP source ranges |
| ingress.http.annotations | object | `{}` | Extra annotations for the ingress |
| ingress.http.auth.authUrl | string | `"http://auth-server.auth-ns.svc.cluster.local/auth"` | URL to an existing authentication service |
| ingress.http.auth.enabled | bool | `false` | Flag to enable auth |
| ingress.http.class | string | `"nginx"` | Which ingress controller to use |
| ingress.http.enabled | bool | `false` | Flag to create an ingress resource for the service |
| ingress.http.hosts | list | `[]` | List of hostnames to match when routing requests |
| ingress.http.https.enabled | bool | `true` | Flag to enable HTTPS |
| ingress.http.https.secretNames | object | `{}` | Map of hostname to TLS secret name |
| ingress.http.whitelist | string | `""` | Allowed client IP source ranges |
| livenessProbe.enabled | bool | `true` | Flag to enabled the probe |
| livenessProbe.failureThreshold | int | `5` | Min consecutive failures for the probe to be considered failed |
| livenessProbe.initialDelaySeconds | int | `60` | Delay before the probe is initiated |
| livenessProbe.periodSeconds | int | `10` | How often to perform the probe |
| livenessProbe.successThreshold | int | `1` | Min consecutive success for the probe to be considered successful |
| livenessProbe.timeoutSeconds | int | `5` | When the probe times out |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| podLabels | object | `{}` | Labels to be added to Feast Job Service pods |
| prometheus.enabled | bool | `true` | Flag to enable scraping of metrics |
| readinessProbe.enabled | bool | `true` | Flag to enabled the probe |
| readinessProbe.failureThreshold | int | `5` | Min consecutive failures for the probe to be considered failed |
| readinessProbe.initialDelaySeconds | int | `20` | Delay before the probe is initiated |
| readinessProbe.periodSeconds | int | `10` | How often to perform the probe |
| readinessProbe.successThreshold | int | `1` | Min consecutive success for the probe to be considered successful |
| readinessProbe.timeoutSeconds | int | `10` | When the probe times out |
| replicaCount | int | `1` | Number of pods that will be created |
| resources | object | `{}` | CPU/memory [resource requests/limit](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container) |
| service.grpc.nodePort | string | `nil` | Port number that each cluster node will listen to |
| service.grpc.port | int | `6568` | Service port for GRPC requests |
| service.grpc.targetPort | int | `6568` | Container port serving GRPC requests |
| service.http.nodePort | string | `nil` | Port number that each cluster node will listen to |
| service.http.port | int | `80` | Service port for HTTP requests |
| service.http.targetPort | int | `8080` | Container port serving HTTP requests and Prometheus metrics |
| service.type | string | `"ClusterIP"` | Kubernetes service type |
