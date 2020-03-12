grafana
=======
The leading tool for querying and visualizing time series and metrics.

Current chart version is `5.0.5`

Source code can be found [here](https://grafana.net)



## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| "grafana.ini".analytics.check_for_updates | bool | `true` |  |
| "grafana.ini".grafana_net.url | string | `"https://grafana.net"` |  |
| "grafana.ini".log.mode | string | `"console"` |  |
| "grafana.ini".paths.data | string | `"/var/lib/grafana/data"` |  |
| "grafana.ini".paths.logs | string | `"/var/log/grafana"` |  |
| "grafana.ini".paths.plugins | string | `"/var/lib/grafana/plugins"` |  |
| "grafana.ini".paths.provisioning | string | `"/etc/grafana/provisioning"` |  |
| admin.existingSecret | string | `""` |  |
| admin.passwordKey | string | `"admin-password"` |  |
| admin.userKey | string | `"admin-user"` |  |
| adminUser | string | `"admin"` |  |
| affinity | object | `{}` |  |
| dashboardProviders | object | `{}` |  |
| dashboards | object | `{}` |  |
| dashboardsConfigMaps | object | `{}` |  |
| datasources | object | `{}` |  |
| deploymentStrategy.type | string | `"RollingUpdate"` |  |
| downloadDashboards.env | object | `{}` |  |
| downloadDashboardsImage.pullPolicy | string | `"IfNotPresent"` |  |
| downloadDashboardsImage.repository | string | `"curlimages/curl"` |  |
| downloadDashboardsImage.tag | string | `"7.68.0"` |  |
| env | object | `{}` |  |
| envFromSecret | string | `""` |  |
| envRenderSecret | object | `{}` |  |
| envValueFrom | object | `{}` |  |
| extraConfigmapMounts | list | `[]` |  |
| extraContainers | string | `""` |  |
| extraEmptyDirMounts | list | `[]` |  |
| extraInitContainers | list | `[]` |  |
| extraSecretMounts | list | `[]` |  |
| extraVolumeMounts | list | `[]` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"grafana/grafana"` |  |
| image.tag | string | `"6.6.2"` |  |
| ingress.annotations | object | `{}` |  |
| ingress.enabled | bool | `false` |  |
| ingress.extraPaths | list | `[]` |  |
| ingress.hosts[0] | string | `"chart-example.local"` |  |
| ingress.labels | object | `{}` |  |
| ingress.path | string | `"/"` |  |
| ingress.tls | list | `[]` |  |
| initChownData.enabled | bool | `true` |  |
| initChownData.image.pullPolicy | string | `"IfNotPresent"` |  |
| initChownData.image.repository | string | `"busybox"` |  |
| initChownData.image.tag | string | `"1.31.1"` |  |
| initChownData.resources | object | `{}` |  |
| ldap.config | string | `""` |  |
| ldap.enabled | bool | `false` |  |
| ldap.existingSecret | string | `""` |  |
| livenessProbe.failureThreshold | int | `10` |  |
| livenessProbe.httpGet.path | string | `"/api/health"` |  |
| livenessProbe.httpGet.port | int | `3000` |  |
| livenessProbe.initialDelaySeconds | int | `60` |  |
| livenessProbe.timeoutSeconds | int | `30` |  |
| namespaceOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| notifiers | object | `{}` |  |
| persistence.accessModes[0] | string | `"ReadWriteOnce"` |  |
| persistence.enabled | bool | `false` |  |
| persistence.finalizers[0] | string | `"kubernetes.io/pvc-protection"` |  |
| persistence.size | string | `"10Gi"` |  |
| persistence.type | string | `"pvc"` |  |
| plugins | list | `[]` |  |
| podDisruptionBudget | object | `{}` |  |
| podPortName | string | `"grafana"` |  |
| rbac.create | bool | `true` |  |
| rbac.extraClusterRoleRules | list | `[]` |  |
| rbac.extraRoleRules | list | `[]` |  |
| rbac.namespaced | bool | `false` |  |
| rbac.pspEnabled | bool | `true` |  |
| rbac.pspUseAppArmor | bool | `true` |  |
| readinessProbe.httpGet.path | string | `"/api/health"` |  |
| readinessProbe.httpGet.port | int | `3000` |  |
| replicas | int | `1` |  |
| resources | object | `{}` |  |
| securityContext.fsGroup | int | `472` |  |
| securityContext.runAsUser | int | `472` |  |
| service.annotations | object | `{}` |  |
| service.labels | object | `{}` |  |
| service.port | int | `80` |  |
| service.portName | string | `"service"` |  |
| service.targetPort | int | `3000` |  |
| service.type | string | `"ClusterIP"` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `nil` |  |
| serviceAccount.nameTest | string | `nil` |  |
| sidecar.dashboards.SCProvider | bool | `true` |  |
| sidecar.dashboards.defaultFolderName | string | `nil` |  |
| sidecar.dashboards.enabled | bool | `false` |  |
| sidecar.dashboards.folder | string | `"/tmp/dashboards"` |  |
| sidecar.dashboards.label | string | `"grafana_dashboard"` |  |
| sidecar.dashboards.provider.allowUiUpdates | bool | `false` |  |
| sidecar.dashboards.provider.disableDelete | bool | `false` |  |
| sidecar.dashboards.provider.folder | string | `""` |  |
| sidecar.dashboards.provider.name | string | `"sidecarProvider"` |  |
| sidecar.dashboards.provider.orgid | int | `1` |  |
| sidecar.dashboards.provider.type | string | `"file"` |  |
| sidecar.dashboards.searchNamespace | string | `nil` |  |
| sidecar.dashboards.watchMethod | string | `"WATCH"` |  |
| sidecar.datasources.enabled | bool | `false` |  |
| sidecar.datasources.label | string | `"grafana_datasource"` |  |
| sidecar.datasources.searchNamespace | string | `nil` |  |
| sidecar.datasources.watchMethod | string | `"WATCH"` |  |
| sidecar.image | string | `"kiwigrid/k8s-sidecar:0.1.99"` |  |
| sidecar.imagePullPolicy | string | `"IfNotPresent"` |  |
| sidecar.resources | object | `{}` |  |
| smtp.existingSecret | string | `""` |  |
| smtp.passwordKey | string | `"password"` |  |
| smtp.userKey | string | `"user"` |  |
| testFramework.enabled | bool | `true` |  |
| testFramework.image | string | `"bats/bats"` |  |
| testFramework.securityContext | object | `{}` |  |
| testFramework.tag | string | `"v1.1.0"` |  |
| tolerations | list | `[]` |  |
