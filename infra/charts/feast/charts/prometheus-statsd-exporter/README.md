prometheus-statsd-exporter
==========================
A Helm chart for prometheus statsd-exporter Scrape metrics stored statsd

Current chart version is `0.1.2`

Source code can be found [here](https://github.com/prometheus/statsd_exporter)



## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"prom/statsd-exporter"` |  |
| image.tag | string | `"v0.12.1"` |  |
| persistentVolume.accessModes[0] | string | `"ReadWriteOnce"` |  |
| persistentVolume.annotations | object | `{}` |  |
| persistentVolume.claimName | string | `"prometheus-statsd-exporter"` |  |
| persistentVolume.enabled | bool | `true` |  |
| persistentVolume.existingClaim | string | `""` |  |
| persistentVolume.mountPath | string | `"/data"` |  |
| persistentVolume.name | string | `"storage-volume"` |  |
| persistentVolume.size | string | `"20Gi"` |  |
| persistentVolume.storageClass | object | `{}` |  |
| persistentVolume.subPath | string | `""` |  |
| service.annotations | object | `{}` |  |
| service.clusterIP | string | `""` |  |
| service.externalIPs | list | `[]` |  |
| service.labels | object | `{}` |  |
| service.loadBalancerIP | string | `""` |  |
| service.loadBalancerSourceRanges | list | `[]` |  |
| service.metricsPort | int | `9102` |  |
| service.servicePort | int | `80` |  |
| service.statsdPort | int | `9125` |  |
| service.type | string | `"ClusterIP"` |  |
| serviceAccount.componentName | string | `"prometheus-statsd-exporter"` |  |
| serviceAccount.enable | bool | `false` |  |
| statsdexporter.affinity | object | `{}` |  |
| statsdexporter.extraArgs | object | `{}` |  |
| statsdexporter.ingress.enabled | bool | `false` |  |
| statsdexporter.nodeSelector | object | `{}` |  |
| statsdexporter.podAnnotations."prometheus.io/path" | string | `"/metrics"` |  |
| statsdexporter.podAnnotations."prometheus.io/port" | string | `"9102"` |  |
| statsdexporter.podAnnotations."prometheus.io/scrape" | string | `"true"` |  |
| statsdexporter.replicaCount | int | `1` |  |
| statsdexporter.resources | object | `{}` |  |
| statsdexporter.tolerations | object | `{}` |  |
