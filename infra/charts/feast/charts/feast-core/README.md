feast-core
==========
Feast Core registers feature specifications and manage ingestion jobs.

Current chart version is `0.4.6`





## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| "application-override.yaml" | object | `{}` | [Configuration override](https://github.com/gojek/feast/blob/v0.4-branch/core/src/main/resources/application.yml) for Feast Core |
| gcpServiceAccount.enabled | bool | `false` | Flag to use [service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) JSON key |
| gcpServiceAccount.existingSecret.key | string | `"credentials.json"` | Key in the secret data (file name of the service account) |
| gcpServiceAccount.existingSecret.name | string | `"feast-gcp-service-account"` | Name of the existing secret containing the service account |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.repository | string | `"gcr.io/kf-feast/feast-core"` | Docker image repository |
| image.tag | string | `"0.4.6"` | Image tag |
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
| javaOpts | string | `nil` | [JVM options](https://docs.oracle.com/cd/E22289_01/html/821-1274/configuring-the-default-jvm-and-java-arguments.html). It is advised for better performance to set the min and max heap: <br> `-Xms2048m -Xmx2048m` |
| livenessProbe.enabled | bool | `true` | Flag to enabled the probe |
| livenessProbe.failureThreshold | int | `5` | Min consecutive failures for the probe to be considered failed |
| livenessProbe.initialDelaySeconds | int | `60` | Delay before the probe is initiated |
| livenessProbe.periodSeconds | int | `10` | How often to perform the probe |
| livenessProbe.successThreshold | int | `1` | Min consecutive success for the probe to be considered successful |
| livenessProbe.timeoutSeconds | int | `5` | When the probe times out |
| logLevel | string | `"WARN"` | Default log level, use either one of `DEBUG`, `INFO`, `WARN` or `ERROR` |
| logType | string | `"Console"` | Log format, either `JSON` or `Console` |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| postgresql.existingSecret | string | `nil` | Existing secret to use for authenticating to Postgres |
| prometheus.enabled | bool | `true` | Flag to enable scraping of Feast Core metrics |
| readinessProbe.enabled | bool | `true` | Flag to enabled the probe |
| readinessProbe.failureThreshold | int | `5` | Min consecutive failures for the probe to be considered failed |
| readinessProbe.initialDelaySeconds | int | `20` | Delay before the probe is initiated |
| readinessProbe.periodSeconds | int | `10` | How often to perform the probe |
| readinessProbe.successThreshold | int | `1` | Min consecutive success for the probe to be considered successful |
| readinessProbe.timeoutSeconds | int | `10` | When the probe times out |
| replicaCount | int | `1` | Number of pods that will be created |
| resources | object | `{}` | CPU/memory [resource requests/limit](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container) |
| service.grpc.nodePort | string | `nil` | Port number that each cluster node will listen to |
| service.grpc.port | int | `6565` | Service port for GRPC requests |
| service.grpc.targetPort | int | `6565` | Container port serving GRPC requests |
| service.http.nodePort | string | `nil` | Port number that each cluster node will listen to |
| service.http.port | int | `80` | Service port for HTTP requests |
| service.http.targetPort | int | `8080` | Container port serving HTTP requests |
| service.type | string | `"ClusterIP"` | Kubernetes service type |
