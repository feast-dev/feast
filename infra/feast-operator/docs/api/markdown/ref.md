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


#### FeastInitOptions



FeastInitOptions defines how to run a `feast init`.

_Appears in:_
- [FeastProjectDir](#feastprojectdir)

| Field | Description |
| --- | --- |
| `minimal` _boolean_ |  |
| `template` _string_ | Template for the created project |


#### FeastProjectDir



FeastProjectDir defines how to create the feast project directory.

_Appears in:_
- [FeatureStoreSpec](#featurestorespec)

| Field | Description |
| --- | --- |
| `git` _[GitCloneOptions](#gitcloneoptions)_ |  |
| `init` _[FeastInitOptions](#feastinitoptions)_ |  |


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



FeatureStoreServices defines the desired feast services. An ephemeral onlineStore feature server is deployed by default.

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
| `feastProjectDir` _[FeastProjectDir](#feastprojectdir)_ |  |
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


#### GitCloneOptions



GitCloneOptions describes how a clone should be performed.

_Appears in:_
- [FeastProjectDir](#feastprojectdir)

| Field | Description |
| --- | --- |
| `url` _string_ | The repository URL to clone from. |
| `ref` _string_ | Reference to a branch / tag / commit |
| `configs` _object (keys:string, values:string)_ | Configs passed to git via `-c`
e.g. http.sslVerify: 'false'
OR 'url."https://api:\${TOKEN}@github.com/".insteadOf': 'https://github.com/' |
| `featureRepoPath` _string_ | FeatureRepoPath is the relative path to the feature repo subdirectory. Default is 'feature_repo'. |
| `env` _[EnvVar](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envvar-v1-core)_ |  |
| `envFrom` _[EnvFromSource](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#envfromsource-v1-core)_ |  |


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



LocalRegistryConfig configures the registry service

_Appears in:_
- [Registry](#registry)

| Field | Description |
| --- | --- |
| `server` _[ServerConfigs](#serverconfigs)_ | Creates a registry server container |
| `persistence` _[RegistryPersistence](#registrypersistence)_ |  |


#### OfflineStore



OfflineStore configures the offline store service

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
| `type` _string_ | Type of the persistence type you want to use. |
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



OnlineStore configures the online store service

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
| `type` _string_ | Type of the persistence type you want to use. |
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



PvcConfig defines the settings for a persistent file store based on PVCs.
We can refer to an existing PVC using the `Ref` field, or create a new one using the `Create` field.

_Appears in:_
- [OfflineStoreFilePersistence](#offlinestorefilepersistence)
- [OnlineStoreFilePersistence](#onlinestorefilepersistence)
- [RegistryFilePersistence](#registryfilepersistence)

| Field | Description |
| --- | --- |
| `ref` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#localobjectreference-v1-core)_ | Reference to an existing field |
| `create` _[PvcCreate](#pvccreate)_ | Settings for creating a new PVC |
| `mountPath` _string_ | MountPath within the container at which the volume should be mounted.
Must start by "/" and cannot contain ':'. |


#### PvcCreate



PvcCreate defines the immutable settings to create a new PVC mounted at the given path.
The PVC name is the same as the associated deployment & feast service name.

_Appears in:_
- [PvcConfig](#pvcconfig)

| Field | Description |
| --- | --- |
| `accessModes` _[PersistentVolumeAccessMode](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#persistentvolumeaccessmode-v1-core) array_ | AccessModes k8s persistent volume access modes. Defaults to ["ReadWriteOnce"]. |
| `storageClassName` _string_ | StorageClassName is the name of an existing StorageClass to which this persistent volume belongs. Empty value
means that this volume does not belong to any StorageClass and the cluster default will be used. |
| `resources` _[VolumeResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#volumeresourcerequirements-v1-core)_ | Resources describes the storage resource requirements for a volume.
Default requested storage size depends on the associated service:
- 10Gi for offline store
- 5Gi for online store
- 5Gi for registry |


#### Registry



Registry configures the registry service. One selection is required. Local is the default setting.

_Appears in:_
- [FeatureStoreServices](#featurestoreservices)

| Field | Description |
| --- | --- |
| `local` _[LocalRegistryConfig](#localregistryconfig)_ |  |
| `remote` _[RemoteRegistryConfig](#remoteregistryconfig)_ |  |


#### RegistryDBStorePersistence



RegistryDBStorePersistence configures the DB store persistence for the registry service

_Appears in:_
- [RegistryPersistence](#registrypersistence)

| Field | Description |
| --- | --- |
| `type` _string_ | Type of the persistence type you want to use. |
| `secretRef` _[LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#localobjectreference-v1-core)_ | Data store parameters should be placed as-is from the "feature_store.yaml" under the secret key. "registry_type" & "type" fields should be removed. |
| `secretKeyName` _string_ | By default, the selected store "type" is used as the SecretKeyName |


#### RegistryFilePersistence



RegistryFilePersistence configures the file-based persistence for the registry service

_Appears in:_
- [RegistryPersistence](#registrypersistence)

| Field | Description |
| --- | --- |
| `path` _string_ |  |
| `pvc` _[PvcConfig](#pvcconfig)_ |  |
| `s3_additional_kwargs` _map[string]string_ |  |


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


