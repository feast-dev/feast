feast-core
==========
Feast Core registers feature specifications and manage ingestion jobs.

Current chart version is `0.5.0-alpha.1`





## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| "application-generated.yaml".enabled | bool | `true` | Flag to include Helm generated configuration for Feast database URL, Kafka bootstrap servers and jobs metrics host. This is useful for deployment that uses default configuration for Kafka, Postgres and StatsD exporter. Please set `application-override.yaml` to override this configuration. |
| "application-override.yaml" | object | `{"enabled":true}` | Configuration to override the default [application.yaml](https://github.com/gojek/feast/blob/master/core/src/main/resources/application.yml). Will be created as a ConfigMap. `application-override.yaml` has a higher precedence than `application-secret.yaml` |
| "application-secret.yaml" | object | `{"enabled":true}` | Configuration to override the default [application.yaml](https://github.com/gojek/feast/blob/master/core/src/main/resources/application.yml). Will be created as a Secret. `application-override.yaml` has a higher precedence than `application-secret.yaml`. It is recommended to either set `application-override.yaml` or `application-secret.yaml` only to simplify config management. |
| "application.yaml".enabled | bool | `true` | Flag to include the default [configuration](https://github.com/gojek/feast/blob/master/core/src/main/resources/application.yml). Please set `application-override.yaml` to override this configuration. |
| envOverrides | object | `{}` | Extra environment variables to set |
| gcpProjectId | string | `""` | Project ID to use when using Google Cloud services such as BigQuery, Cloud Storage and Dataflow |
| gcpServiceAccount.enabled | bool | `false` | Flag to use [service account](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) JSON key |
| gcpServiceAccount.existingSecret.key | string | `"credentials.json"` | Key in the secret data (file name of the service account) |
| gcpServiceAccount.existingSecret.name | string | `"feast-gcp-service-account"` | Name of the existing secret containing the service account |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.repository | string | `"gcr.io/kf-feast/feast-core"` | Docker image repository |
| image.tag | string | `"dev"` | Image tag |
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
| javaOpts | string | `nil` | [JVM options](https://docs.oracle.com/cd/E22289_01/html/821-1274/configuring-the-default-jvm-and-java-arguments.html). For better performance, it is advised to set the min and max heap: <br> `-Xms2048m -Xmx2048m` |
| livenessProbe.enabled | bool | `true` | Flag to enabled the probe |
| livenessProbe.failureThreshold | int | `5` | Min consecutive failures for the probe to be considered failed |
| livenessProbe.initialDelaySeconds | int | `60` | Delay before the probe is initiated |
| livenessProbe.periodSeconds | int | `10` | How often to perform the probe |
| livenessProbe.successThreshold | int | `1` | Min consecutive success for the probe to be considered successful |
| livenessProbe.timeoutSeconds | int | `5` | When the probe times out |
| logLevel | string | `"WARN"` | Default log level, use either one of `DEBUG`, `INFO`, `WARN` or `ERROR` |
| logType | string | `"Console"` | Log format, either `JSON` or `Console` |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| postgresql.existingSecret | string | `""` | Existing secret to use for authenticating to Postgres |
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
| service.http.targetPort | int | `8080` | Container port serving HTTP requests and Prometheus metrics |
| service.type | string | `"ClusterIP"` | Kubernetes service type |
