# feature-server

![Version: 0.65.0](https://img.shields.io/badge/Version-0.65.0-informational?style=flat-square) ![AppVersion: v0.65.0](https://img.shields.io/badge/AppVersion-v0.65.0-informational?style=flat-square)

Feast Feature Server: Online feature serving service for Feast

**Homepage:** <https://github.com/feast-dev/feast>

## Migration from Java chart

This chart now deploys the Python-based feature server instead of the Java-based one.

### Removed values (no equivalent)

These Java-specific values have been removed. Delete them from your values files:

| Removed value | Reason |
|---|---|
| `javaOpts` | Python server, no JVM |
| `logType`, `logLevel` | Java logging config, not applicable |
| `transformationService.*` | Use the separate `transformation-service` subchart |
| `application.yaml`, `application-generated.yaml` | Java Spring config, not applicable |
| `application-override.yaml` | Replaced by `feature_store_yaml_base64` or `existingSecret` |
| `application-secret.yaml` | Replaced by `feature_store_yaml_base64` or `existingSecret` |

### Changed values

| Old value | New value | Notes |
|---|---|---|
| `service.grpc.port` | `service.port` | Protocol changed from gRPC to HTTP |
| `service.grpc.targetPort` | (removed) | `targetPort` now uses the named port `http` |
| `service.grpc.nodePort` | `service.nodePort` | Flat key |
| `ingress.grpc.*` | (removed) | Python server is HTTP-only |
| `ingress.http.class` | (removed) | Use `ingress.http.ingressClassName` or `annotations` |
| `ingress.http.auth.*` | (removed) | Use `ingress.http.annotations` for controller-specific auth |
| `ingress.http.whitelist` | (removed) | Use `ingress.http.annotations` for controller-specific whitelist |
| `image.repository` | `image.repository` | Changed from `feature-server-java` to `feature-server` |

### New values

| Value | Purpose |
|---|---|
| `feature_store_yaml_base64` | Base64-encoded `feature_store.yaml`, stored in a K8s Secret |
| `existingSecret` | Reference a pre-created Secret instead of providing inline config |
| `ingress.http.ingressClassName` | Support for `networking.k8s.io/v1` IngressClass |

### What still works

- `ingress.http.*` — same value structure, now uses `networking.k8s.io/v1`
- `secrets` — volume mounts for additional Kubernetes secrets
- `envOverrides` — extra environment variables
- `service.type`, `service.loadBalancerIP`, `service.loadBalancerSourceRanges`
- All probe settings (`livenessProbe.*`, `readinessProbe.*`)

## Installation

### Option A: Using an existing Secret (recommended)

Create the Secret outside of Helm so that credentials never pass through Helm values or release metadata:

```bash
kubectl create secret generic my-feast-config \
  --from-literal=feature_store_yaml_base64=$(base64 < feature_store.yaml)
```

Then reference it during install:
```bash
helm install feast-feature-server . --set existingSecret=my-feast-config
```

This is the recommended approach when `feature_store.yaml` contains registry or online-store credentials.

### Option B: Using inline config

```bash
helm install feast-feature-server . --set feature_store_yaml_base64=$(base64 < feature_store.yaml)
```

> **Note:** When using `--set`, the base64-encoded config is stored in Helm release metadata and is retrievable via `helm get values`. Use Option A or an external secrets operator (e.g. SealedSecrets, ExternalSecrets) for production deployments with sensitive credentials.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| envOverrides | object | `{}` | Extra environment variables to set |
| existingSecret | string | `""` | Name of an existing Secret containing key `feature_store_yaml_base64` with base64-encoded config |
| feature_store_yaml_base64 | string | `""` | [required] a base64 encoded version of feature_store.yaml (stored in a K8s Secret) |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.repository | string | `"quay.io/feastdev/feature-server"` | Docker image for Feature Server repository |
| image.tag | string | `"0.65.0"` | Image tag |
| ingress.http.annotations | object | `{}` | Extra annotations for the ingress (use for controller-specific settings) |
| ingress.http.enabled | bool | `false` | Flag to create an ingress resource for the service |
| ingress.http.hosts | list | `[]` | List of hostnames to match when routing requests |
| ingress.http.https.enabled | bool | `true` | Flag to enable HTTPS |
| ingress.http.https.secretNames | object | `{}` | Map of hostname to TLS secret name |
| ingress.http.ingressClassName | string | `nil` | IngressClass resource name |
| livenessProbe.enabled | bool | `true` | Flag to enabled the probe |
| livenessProbe.failureThreshold | int | `5` | Min consecutive failures for the probe to be considered failed |
| livenessProbe.initialDelaySeconds | int | `60` | Delay before the probe is initiated |
| livenessProbe.periodSeconds | int | `10` | How often to perform the probe |
| livenessProbe.successThreshold | int | `1` | Min consecutive success for the probe to be considered successful |
| livenessProbe.timeoutSeconds | int | `5` | When the probe times out |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| podAnnotations | object | `{}` | Annotations to be added to Feature Server pods |
| podLabels | object | `{}` | Labels to be added to Feature Server pods |
| readinessProbe.enabled | bool | `true` | Flag to enabled the probe |
| readinessProbe.failureThreshold | int | `5` | Min consecutive failures for the probe to be considered failed |
| readinessProbe.initialDelaySeconds | int | `15` | Delay before the probe is initiated |
| readinessProbe.periodSeconds | int | `10` | How often to perform the probe |
| readinessProbe.successThreshold | int | `1` | Min consecutive success for the probe to be considered successful |
| readinessProbe.timeoutSeconds | int | `10` | When the probe times out |
| replicaCount | int | `1` | Number of pods that will be created |
| resources | object | `{}` | CPU/memory [resource requests/limit](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#resource-requests-and-limits-of-pod-and-container) |
| secrets | list | `[]` | List of Kubernetes secrets to be mounted on /etc/secrets/\<secret name\> |
| service.loadBalancerIP | string | `nil` | Specify a load balancer IP if service type is LoadBalancer |
| service.loadBalancerSourceRanges | list | `[]` | Optionally restrict load balancer traffic to specified IPs |
| service.nodePort | string | `nil` | Port number that each cluster node will listen to |
| service.port | int | `6566` | Service port |
| service.type | string | `"ClusterIP"` | Kubernetes service type |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.14.2](https://github.com/norwoodj/helm-docs/releases/v1.14.2)
