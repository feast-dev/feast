# API Reference

## Packages
- [feast.dev/v1alpha1](#feastdevv1alpha1)


## feast.dev/v1alpha1

Package v1alpha1 contains API Schema definitions for the  v1alpha1 API group

### Resource Types
- [FeatureStore](#featurestore)



#### AuthzConfig



AuthzConfig defines the authorization settings for the deployed Feast services.

_Appears in:_
- [FeatureStoreSpec](#featurestorespec)

| Field | Description |
| --- | --- |
| `kubernetes` _[KubernetesAuthz](#kubernetesauthz)_ |  |
| `oidc` _[OidcAuthz](#oidcauthz)_ |  |


#### ContainerConfigs



ContainerConfigs k8s container settings for the server

_Appears in:_
- [ServerConfigs](#serverconfigs)

| Field | Description |
| --- | --- |
| `image` _string_ |  |
| `env` _[EnvVar](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envvar-v1-core)_ |  |
| `envFrom` _[EnvFromSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envfromsource-v1-core)_ |  |
| `imagePullPolicy` _[PullPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#pullpolicy-v1-core)_ |  |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#resourcerequirements-v1-core)_ |  |


#### DefaultCtrConfigs



DefaultCtrConfigs k8s container settings that are applied by default

_Appears in:_
- [ContainerConfigs](#containerconfigs)
- [ServerConfigs](#serverconfigs)

| Field | Description |
| --- | --- |
| `image` _string_ |  |


#### FeatureStore



FeatureStore is the Schema for the featurestores API



| Field | Description |
| --- | --- |
| `apiVersion` _string_ | `feast.dev/v1alpha1`
| `kind` _string_ | `FeatureStore`
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |
| `spec` _[FeatureStoreSpec](#featurestorespec)_ |  |
| `status` _[FeatureStoreStatus](#featurestorestatus)_ |  |


#### FeatureStoreRef



FeatureStoreRef defines which existing FeatureStore's registry should be used

_Appears in:_
- [RemoteRegistryConfig](#remoteregistryconfig)

| Field | Description |
| --- | --- |
| `name` _string_ | Name of the FeatureStore |
| `namespace` _string_ | Namespace of the FeatureStore |


#### FeatureStoreServices



FeatureStoreServices defines the desired feast services. An ephemeral registry is deployed by default.

_Appears in:_
- [FeatureStoreSpec](#featurestorespec)

| Field | Description |
| --- | --- |
| `offlineStore` _[OfflineStore](#offlinestore)_ |  |
| `onlineStore` _[OnlineStore](#onlinestore)_ |  |
| `registry` _[Registry](#registry)_ |  |
| `ui` _[ServerConfigs](#serverconfigs)_ | Creates a UI server container |
| `deploymentStrategy` _[DeploymentStrategy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#deploymentstrategy-v1-apps)_ |  |
| `disableInitContainers` _boolean_ | Disable the 'feast repo initialization' initContainer |
| `volumes` _[Volume](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#volume-v1-core) array_ | Volumes specifies the volumes to mount in the FeatureStore deployment. A corresponding `VolumeMount` should be added to whichever feast service(s) require access to said volume(s). |


#### FeatureStoreSpec



FeatureStoreSpec defines the desired state of FeatureStore

_Appears in:_
- [FeatureStore](#featurestore)
- [FeatureStoreStatus](#featurestorestatus)

| Field | Description |
| --- | --- |
| `feastProject` _string_ | FeastProject is the Feast project id. This can be any alphanumeric string with underscores, but it cannot start with an underscore. Required. |
| `services` _[FeatureStoreServices](#featurestoreservices)_ |  |
| `authz` _[AuthzConfig](#authzconfig)_ |  |


#### FeatureStoreStatus



FeatureStoreStatus defines the observed state of FeatureStore

_Appears in:_
- [FeatureStore](#featurestore)

| Field | Description |
| --- | --- |
| `applied` _[FeatureStoreSpec](#featurestorespec)_ | Shows the currently applied feast configuration, including any pertinent defaults |
| `clientConfigMap` _string_ | ConfigMap in this namespace containing a client `feature_store.yaml` for this feast deployment |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#condition-v1-meta) array_ |  |
| `feastVersion` _string_ |  |
| `phase` _string_ |  |
| `serviceHostnames` _[ServiceHostnames](#servicehostnames)_ |  |


#### KubernetesAuthz



KubernetesAuthz provides a way to define the authorization settings using Kubernetes RBAC resources.
https://kubernetes.io/docs/reference/access-authn-authz/rbac/

_Appears in:_
- [AuthzConfig](#authzconfig)

| Field | Description |
| --- | --- |
| `roles` _string array_ | The Kubernetes RBAC roles to be deployed in the same namespace of the FeatureStore.
Roles are managed by the operator and created with an empty list of rules.
See the Feast permission model at https://docs.feast.dev/getting-started/concepts/permission
The feature store admin is not obligated to manage roles using the Feast operator, roles can be managed independently.
This configuration option is only providing a way to automate this procedure.
Important note: the operator cannot ensure that these roles will match the ones used in the configured Feast permissions. |


#### LocalRegistryConfig



LocalRegistryConfig configures the deployed registry service

_Appears in:_
- [Registry](#registry)

| Field | Description |
| --- | --- |
| `server` _[ServerConfigs](#serverconfigs)_ | Creates a registry server container |
| `persistence` _[RegistryPersistence](#registrypersistence)_ |  |


#### OfflineStore



OfflineStore configures the deployed offline store service

_Appears in:_
- [FeatureStoreServices](#featurestoreservices)

| Field | Description |
| --- | --- |
| `server` _[ServerConfigs](#serverconfigs)_ | Creates a remote offline server container |
| `persistence` _[OfflineStorePersistence](#offlinestorepersistence)_ |  |


#### OfflineStoreDBStorePersistence



OfflineStoreDBStorePersistence configures the DB store persistence for the offline store service

_Appears in:_
- [OfflineStorePersistence](#offlinestorepersistence)

| Field | Description |
| --- | --- |
| `type` _string_ | Type of the persistence type you want to use. Allowed values are: snowflake.offline, bigquery, redshift, spark, postgres, trino, redis, athena, mssql |
| `secretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#localobjectreference-v1-core)_ | Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed. |
| `secretKeyName` _string_ | By default, the selected store "type" is used as the SecretKeyName |


#### OfflineStoreFilePersistence



OfflineStoreFilePersistence configures the file-based persistence for the offline store service

_Appears in:_
- [OfflineStorePersistence](#offlinestorepersistence)

| Field | Description |
| --- | --- |
| `type` _string_ |  |
| `pvc` _[PvcConfig](#pvcconfig)_ |  |


#### OfflineStorePersistence



OfflineStorePersistence configures the persistence settings for the offline store service

_Appears in:_
- [OfflineStore](#offlinestore)

| Field | Description |
| --- | --- |
| `file` _[OfflineStoreFilePersistence](#offlinestorefilepersistence)_ |  |
| `store` _[OfflineStoreDBStorePersistence](#offlinestoredbstorepersistence)_ |  |


#### OidcAuthz



OidcAuthz defines the authorization settings for deployments using an Open ID Connect identity provider.
https://auth0.com/docs/authenticate/protocols/openid-connect-protocol

_Appears in:_
- [AuthzConfig](#authzconfig)

| Field | Description |
| --- | --- |
| `secretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#localobjectreference-v1-core)_ |  |


#### OnlineStore



OnlineStore configures the deployed online store service

_Appears in:_
- [FeatureStoreServices](#featurestoreservices)

| Field | Description |
| --- | --- |
| `server` _[ServerConfigs](#serverconfigs)_ | Creates a feature server container |
| `persistence` _[OnlineStorePersistence](#onlinestorepersistence)_ |  |


#### OnlineStoreDBStorePersistence



OnlineStoreDBStorePersistence configures the DB store persistence for the online store service

_Appears in:_
- [OnlineStorePersistence](#onlinestorepersistence)

| Field | Description |
| --- | --- |
| `type` _string_ | Type of the persistence type you want to use. Allowed values are: snowflake.online, redis, ikv, datastore, dynamodb, bigtable, postgres, cassandra, mysql, hazelcast, singlestore, hbase, elasticsearch, qdrant, couchbase, milvus |
| `secretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#localobjectreference-v1-core)_ | Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed. |
| `secretKeyName` _string_ | By default, the selected store "type" is used as the SecretKeyName |


#### OnlineStoreFilePersistence



OnlineStoreFilePersistence configures the file-based persistence for the online store service

_Appears in:_
- [OnlineStorePersistence](#onlinestorepersistence)

| Field | Description |
| --- | --- |
| `path` _string_ |  |
| `pvc` _[PvcConfig](#pvcconfig)_ |  |


#### OnlineStorePersistence



OnlineStorePersistence configures the persistence settings for the online store service

_Appears in:_
- [OnlineStore](#onlinestore)

| Field | Description |
| --- | --- |
| `file` _[OnlineStoreFilePersistence](#onlinestorefilepersistence)_ |  |
| `store` _[OnlineStoreDBStorePersistence](#onlinestoredbstorepersistence)_ |  |


#### OptionalCtrConfigs



OptionalCtrConfigs k8s container settings that are optional

_Appears in:_
- [ContainerConfigs](#containerconfigs)
- [ServerConfigs](#serverconfigs)

| Field | Description |
| --- | --- |
| `env` _[EnvVar](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envvar-v1-core)_ |  |
| `envFrom` _[EnvFromSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envfromsource-v1-core)_ |  |
| `imagePullPolicy` _[PullPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#pullpolicy-v1-core)_ |  |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#resourcerequirements-v1-core)_ |  |


#### PvcConfig

_Underlying type:_ `[struct{Ref *k8s.io/api/core/v1.LocalObjectReference "json:\"ref,omitempty\""; Create *PvcCreate "json:\"create,omitempty\""; MountPath string "json:\"mountPath\""}](#struct{ref-*k8sioapicorev1localobjectreference-"json:\"ref,omitempty\"";-create-*pvccreate-"json:\"create,omitempty\"";-mountpath-string-"json:\"mountpath\""})`

PvcConfig defines the settings for a persistent file store based on PVCs.
We can refer to an existing PVC using the `Ref` field, or create a new one using the `Create` field.

_Appears in:_
- [OfflineStoreFilePersistence](#offlinestorefilepersistence)
- [OnlineStoreFilePersistence](#onlinestorefilepersistence)





#### Registry



Registry configures the registry service. One selection is required. Local is the default setting.

_Appears in:_
- [FeatureStoreServices](#featurestoreservices)

| Field | Description |
| --- | --- |
| `local` _[LocalRegistryConfig](#localregistryconfig)_ |  |
| `remote` _[RemoteRegistryConfig](#remoteregistryconfig)_ |  |


#### RegistryDBStorePersistence

_Underlying type:_ `[struct{Type string "json:\"type\""; SecretRef k8s.io/api/core/v1.LocalObjectReference "json:\"secretRef\""; SecretKeyName string "json:\"secretKeyName,omitempty\""}](#struct{type-string-"json:\"type\"";-secretref-k8sioapicorev1localobjectreference-"json:\"secretref\"";-secretkeyname-string-"json:\"secretkeyname,omitempty\""})`

RegistryDBStorePersistence configures the DB store persistence for the registry service

_Appears in:_
- [RegistryPersistence](#registrypersistence)



#### RegistryFilePersistence

_Underlying type:_ `[struct{Path string "json:\"path,omitempty\""; PvcConfig *PvcConfig "json:\"pvc,omitempty\""; S3AdditionalKwargs *map[string]string "json:\"s3_additional_kwargs,omitempty\""}](#struct{path-string-"json:\"path,omitempty\"";-pvcconfig-*pvcconfig-"json:\"pvc,omitempty\"";-s3additionalkwargs-*map[string]string-"json:\"s3_additional_kwargs,omitempty\""})`

RegistryFilePersistence configures the file-based persistence for the registry service

_Appears in:_
- [RegistryPersistence](#registrypersistence)



#### RegistryPersistence



RegistryPersistence configures the persistence settings for the registry service

_Appears in:_
- [LocalRegistryConfig](#localregistryconfig)

| Field | Description |
| --- | --- |
| `file` _[RegistryFilePersistence](#registryfilepersistence)_ |  |
| `store` _[RegistryDBStorePersistence](#registrydbstorepersistence)_ |  |


#### RemoteRegistryConfig



RemoteRegistryConfig points to a remote feast registry server. When set, the operator will not deploy a registry for this FeatureStore CR.
Instead, this FeatureStore CR's online/offline services will use a remote registry. One selection is required.

_Appears in:_
- [Registry](#registry)

| Field | Description |
| --- | --- |
| `hostname` _string_ | Host address of the remote registry service - <domain>:<port>, e.g. `registry.<namespace>.svc.cluster.local:80` |
| `feastRef` _[FeatureStoreRef](#featurestoreref)_ | Reference to an existing `FeatureStore` CR in the same k8s cluster. |
| `tls` _[TlsRemoteRegistryConfigs](#tlsremoteregistryconfigs)_ |  |


#### SecretKeyNames



SecretKeyNames defines the secret key names for the TLS key and cert.

_Appears in:_
- [TlsConfigs](#tlsconfigs)

| Field | Description |
| --- | --- |
| `tlsCrt` _string_ | defaults to "tls.crt" |
| `tlsKey` _string_ | defaults to "tls.key" |


#### ServerConfigs



ServerConfigs creates a server for the feast service, with specified container configurations.

_Appears in:_
- [FeatureStoreServices](#featurestoreservices)
- [LocalRegistryConfig](#localregistryconfig)
- [OfflineStore](#offlinestore)
- [OnlineStore](#onlinestore)

| Field | Description |
| --- | --- |
| `image` _string_ |  |
| `env` _[EnvVar](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envvar-v1-core)_ |  |
| `envFrom` _[EnvFromSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envfromsource-v1-core)_ |  |
| `imagePullPolicy` _[PullPolicy](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#pullpolicy-v1-core)_ |  |
| `resources` _[ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#resourcerequirements-v1-core)_ |  |
| `tls` _[TlsConfigs](#tlsconfigs)_ |  |
| `logLevel` _string_ | LogLevel sets the logging level for the server
Allowed values: "debug", "info", "warning", "error", "critical". |
| `volumeMounts` _[VolumeMount](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#volumemount-v1-core) array_ | VolumeMounts defines the list of volumes that should be mounted into the feast container.
This allows attaching persistent storage, config files, secrets, or other resources
required by the Feast components. Ensure that each volume mount has a corresponding
volume definition in the Volumes field. |


#### ServiceHostnames



ServiceHostnames defines the service hostnames in the format of <domain>:<port>, e.g. example.svc.cluster.local:80

_Appears in:_
- [FeatureStoreStatus](#featurestorestatus)

| Field | Description |
| --- | --- |
| `offlineStore` _string_ |  |
| `onlineStore` _string_ |  |
| `registry` _string_ |  |
| `ui` _string_ |  |


#### TlsConfigs



TlsConfigs configures server TLS for a feast service. in an openshift cluster, this is configured by default using service serving certificates.

_Appears in:_
- [ServerConfigs](#serverconfigs)

| Field | Description |
| --- | --- |
| `secretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#localobjectreference-v1-core)_ | references the local k8s secret where the TLS key and cert reside |
| `secretKeyNames` _[SecretKeyNames](#secretkeynames)_ |  |
| `disable` _boolean_ | will disable TLS for the feast service. useful in an openshift cluster, for example, where TLS is configured by default |


#### TlsRemoteRegistryConfigs



TlsRemoteRegistryConfigs configures client TLS for a remote feast registry. in an openshift cluster, this is configured by default when the remote feast registry is using service serving certificates.

_Appears in:_
- [RemoteRegistryConfig](#remoteregistryconfig)

| Field | Description |
| --- | --- |
| `configMapRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#localobjectreference-v1-core)_ | references the local k8s configmap where the TLS cert resides |
| `certName` _string_ | defines the configmap key name for the client TLS cert. |


