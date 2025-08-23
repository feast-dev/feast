# feast-operator

![Version: 0.52.0](https://img.shields.io/badge/Version-0.52.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.52.0](https://img.shields.io/badge/AppVersion-0.52.0-informational?style=flat-square)

Current chart version is `0.52.0`

A Kubernetes operator for managing Feast feature stores

## Installation
```bash
helm repo add feast https://feast-dev.github.io/feast
helm repo update
helm install feast-operator feast/feast-operator \
  --namespace features --create-namespace
```

```bash
# From source (within the Feast repo)
cd infra/charts/feast-operator
helm install feast-operator . \
  --namespace features --create-namespace
```

## CRDs and Operator
- This chart installs/updates the Feast Operator.
- CRDs can be installed by setting `crds.install=true` (default).
- Manage `FeatureStore` custom resources separately (your app chart/manifests, GitOps).

## Service Account & RBAC
- Creates a dedicated ServiceAccount by default (`serviceAccount.create=true`).
- Includes ClusterRole/Binding and a namespaced Role/Binding for leader election.

## Metrics and Monitoring
- `metrics.enabled` exposes an HTTPS metrics service on port 8443.
- `prometheus.serviceMonitor.enabled` creates a ServiceMonitor (only rendered if the CRD exists in the cluster).

## Uninstall
```bash
helm uninstall feast-operator -n features
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| advanced.healthProbeBindAddress | string | `":8081"` |  |
| advanced.leaderElection | bool | `true` |  |
| advanced.metricsBindAddress | string | `":8443"` |  |
| advanced.terminationGracePeriodSeconds | int | `10` |  |
| commonAnnotations | object | `{}` |  |
| commonLabels | object | `{}` |  |
| crds.install | bool | `true` |  |
| global.imagePullSecrets | list | `[]` |  |
| global.imageRegistry | string | `""` |  |
| metrics.enabled | bool | `true` |  |
| metrics.service.annotations | object | `{}` |  |
| metrics.service.port | int | `8443` |  |
| metrics.service.targetPort | int | `8443` |  |
| metrics.service.type | string | `"ClusterIP"` |  |
| namePrefix | string | `""` |  |
| namespace.create | bool | `true` |  |
| namespace.labels.control-plane | string | `"controller-manager"` |  |
| namespace.name | string | `""` |  |
| operator.affinity | object | `{}` |  |
| operator.args | list | `[]` |  |
| operator.containerSecurityContext.allowPrivilegeEscalation | bool | `false` |  |
| operator.containerSecurityContext.capabilities.drop[0] | string | `"ALL"` |  |
| operator.env.relatedImageCronJob | string | `"quay.io/openshift/origin-cli:4.17"` |  |
| operator.env.relatedImageFeatureServer | string | `"quay.io/feastdev/feature-server:0.52.0"` |  |
| operator.healthcheck.livenessProbe.initialDelaySeconds | int | `15` |  |
| operator.healthcheck.livenessProbe.periodSeconds | int | `20` |  |
| operator.healthcheck.port | int | `8081` |  |
| operator.healthcheck.readinessProbe.initialDelaySeconds | int | `5` |  |
| operator.healthcheck.readinessProbe.periodSeconds | int | `10` |  |
| operator.image.pullPolicy | string | `"IfNotPresent"` |  |
| operator.image.repository | string | `"quay.io/feastdev/feast-operator"` |  |
| operator.image.tag | string | `"0.52.0"` |  |
| operator.nodeSelector | object | `{}` |  |
| operator.podAnnotations."kubectl.kubernetes.io/default-container" | string | `"manager"` |  |
| operator.podLabels | object | `{}` |  |
| operator.replicaCount | int | `1` |  |
| operator.resources.limits.cpu | string | `"1000m"` |  |
| operator.resources.limits.memory | string | `"256Mi"` |  |
| operator.resources.requests.cpu | string | `"10m"` |  |
| operator.resources.requests.memory | string | `"64Mi"` |  |
| operator.securityContext.runAsNonRoot | bool | `true` |  |
| operator.tolerations | list | `[]` |  |
| prometheus.serviceMonitor.enabled | bool | `false` |  |
| prometheus.serviceMonitor.interval | string | `"30s"` |  |
| prometheus.serviceMonitor.labels | object | `{}` |  |
| prometheus.serviceMonitor.timeout | string | `"10s"` |  |
| rbac.create | bool | `true` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| storage.defaultAccessModes[0] | string | `"ReadWriteOnce"` |  |
| storage.defaultStorageClass | string | `""` |  |
| storage.reclaimPolicy | string | `""` |  |

## Source Code

* <https://github.com/feast-dev/feast>
* <https://github.com/feast-dev/feast/tree/master/infra/feast-operator>
## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Feast Community | <maintainers@feast.dev> | <https://feast.dev/> |
