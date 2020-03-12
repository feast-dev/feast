kafka
=====
Apache Kafka is publish-subscribe messaging rethought as a distributed commit log.

Current chart version is `0.20.8`

Source code can be found [here](https://kafka.apache.org/)

## Chart Requirements

| Repository | Name | Version |
|------------|------|---------|
| https://kubernetes-charts-incubator.storage.googleapis.com/ | zookeeper | 2.1.0 |

## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalPorts | object | `{}` |  |
| affinity | object | `{}` |  |
| configJob.backoffLimit | int | `6` |  |
| configurationOverrides."confluent.support.metrics.enable" | bool | `false` |  |
| envOverrides | object | `{}` |  |
| external.distinct | bool | `false` |  |
| external.dns.useExternal | bool | `true` |  |
| external.dns.useInternal | bool | `false` |  |
| external.domain | string | `"cluster.local"` |  |
| external.enabled | bool | `false` |  |
| external.firstListenerPort | int | `31090` |  |
| external.init.image | string | `"lwolf/kubectl_deployer"` |  |
| external.init.imagePullPolicy | string | `"IfNotPresent"` |  |
| external.init.imageTag | string | `"0.4"` |  |
| external.loadBalancerIP | list | `[]` |  |
| external.loadBalancerSourceRanges | list | `[]` |  |
| external.servicePort | int | `19092` |  |
| external.type | string | `"NodePort"` |  |
| headless.port | int | `9092` |  |
| image | string | `"confluentinc/cp-kafka"` |  |
| imagePullPolicy | string | `"IfNotPresent"` |  |
| imageTag | string | `"5.0.1"` |  |
| jmx.configMap.enabled | bool | `true` |  |
| jmx.configMap.overrideConfig | object | `{}` |  |
| jmx.configMap.overrideName | string | `""` |  |
| jmx.port | int | `5555` |  |
| jmx.whitelistObjectNames[0] | string | `"kafka.controller:*"` |  |
| jmx.whitelistObjectNames[1] | string | `"kafka.server:*"` |  |
| jmx.whitelistObjectNames[2] | string | `"java.lang:*"` |  |
| jmx.whitelistObjectNames[3] | string | `"kafka.network:*"` |  |
| jmx.whitelistObjectNames[4] | string | `"kafka.log:*"` |  |
| kafkaHeapOptions | string | `"-Xmx1G -Xms1G"` |  |
| logSubPath | string | `"logs"` |  |
| nodeSelector | object | `{}` |  |
| persistence.enabled | bool | `true` |  |
| persistence.mountPath | string | `"/opt/kafka/data"` |  |
| persistence.size | string | `"1Gi"` |  |
| podAnnotations | object | `{}` |  |
| podDisruptionBudget | object | `{}` |  |
| podLabels | object | `{}` |  |
| podManagementPolicy | string | `"OrderedReady"` |  |
| prometheus.jmx.enabled | bool | `false` |  |
| prometheus.jmx.image | string | `"solsson/kafka-prometheus-jmx-exporter@sha256"` |  |
| prometheus.jmx.imageTag | string | `"a23062396cd5af1acdf76512632c20ea6be76885dfc20cd9ff40fb23846557e8"` |  |
| prometheus.jmx.interval | string | `"10s"` |  |
| prometheus.jmx.port | int | `5556` |  |
| prometheus.jmx.resources | object | `{}` |  |
| prometheus.jmx.scrapeTimeout | string | `"10s"` |  |
| prometheus.kafka.affinity | object | `{}` |  |
| prometheus.kafka.enabled | bool | `false` |  |
| prometheus.kafka.image | string | `"danielqsj/kafka-exporter"` |  |
| prometheus.kafka.imageTag | string | `"v1.2.0"` |  |
| prometheus.kafka.interval | string | `"10s"` |  |
| prometheus.kafka.nodeSelector | object | `{}` |  |
| prometheus.kafka.port | int | `9308` |  |
| prometheus.kafka.resources | object | `{}` |  |
| prometheus.kafka.scrapeTimeout | string | `"10s"` |  |
| prometheus.kafka.tolerations | list | `[]` |  |
| prometheus.operator.enabled | bool | `false` |  |
| prometheus.operator.prometheusRule.enabled | bool | `false` |  |
| prometheus.operator.prometheusRule.namespace | string | `"monitoring"` |  |
| prometheus.operator.prometheusRule.releaseNamespace | bool | `false` |  |
| prometheus.operator.prometheusRule.rules[0].alert | string | `"KafkaNoActiveControllers"` |  |
| prometheus.operator.prometheusRule.rules[0].annotations.message | string | `"The number of active controllers in {{ \"{{\" }} $labels.namespace {{ \"}}\" }} is less than 1. This usually means that some of the Kafka nodes aren't communicating properly. If it doesn't resolve itself you can try killing the pods (one by one whilst monitoring the under-replicated partitions graph)."` |  |
| prometheus.operator.prometheusRule.rules[0].expr | string | `"max(kafka_controller_kafkacontroller_activecontrollercount_value) by (namespace) \u003c 1"` |  |
| prometheus.operator.prometheusRule.rules[0].for | string | `"5m"` |  |
| prometheus.operator.prometheusRule.rules[0].labels.severity | string | `"critical"` |  |
| prometheus.operator.prometheusRule.rules[1].alert | string | `"KafkaMultipleActiveControllers"` |  |
| prometheus.operator.prometheusRule.rules[1].annotations.message | string | `"The number of active controllers in {{ \"{{\" }} $labels.namespace {{ \"}}\" }} is greater than 1. This usually means that some of the Kafka nodes aren't communicating properly. If it doesn't resolve itself you can try killing the pods (one by one whilst monitoring the under-replicated partitions graph)."` |  |
| prometheus.operator.prometheusRule.rules[1].expr | string | `"max(kafka_controller_kafkacontroller_activecontrollercount_value) by (namespace) \u003e 1"` |  |
| prometheus.operator.prometheusRule.rules[1].for | string | `"5m"` |  |
| prometheus.operator.prometheusRule.rules[1].labels.severity | string | `"critical"` |  |
| prometheus.operator.prometheusRule.selector.prometheus | string | `"kube-prometheus"` |  |
| prometheus.operator.serviceMonitor.namespace | string | `"monitoring"` |  |
| prometheus.operator.serviceMonitor.releaseNamespace | bool | `false` |  |
| prometheus.operator.serviceMonitor.selector.prometheus | string | `"kube-prometheus"` |  |
| readinessProbe.failureThreshold | int | `3` |  |
| readinessProbe.initialDelaySeconds | int | `30` |  |
| readinessProbe.periodSeconds | int | `10` |  |
| readinessProbe.successThreshold | int | `1` |  |
| readinessProbe.timeoutSeconds | int | `5` |  |
| replicas | int | `3` |  |
| resources | object | `{}` |  |
| securityContext | object | `{}` |  |
| terminationGracePeriodSeconds | int | `60` |  |
| tolerations | list | `[]` |  |
| topics | list | `[]` |  |
| updateStrategy.type | string | `"OnDelete"` |  |
| zookeeper.affinity | object | `{}` |  |
| zookeeper.enabled | bool | `true` |  |
| zookeeper.env.ZK_HEAP_SIZE | string | `"1G"` |  |
| zookeeper.image.PullPolicy | string | `"IfNotPresent"` |  |
| zookeeper.persistence.enabled | bool | `false` |  |
| zookeeper.port | int | `2181` |  |
| zookeeper.resources | string | `nil` |  |
| zookeeper.url | string | `""` |  |
