# Guide 5 — Security

The operator supports two authorization models via `spec.authz`, plus TLS for all servers.
Authorization is optional — omitting `authz` deploys Feast with no access control.

---

## Kubernetes RBAC authorization

Kubernetes RBAC authorization uses ServiceAccount tokens. The operator creates
`ClusterRole`s for each named role you declare and binds them to ServiceAccounts. Feast
servers enforce these roles on every API call.

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-rbac
spec:
  feastProject: feast_rbac
  authz:
    kubernetes:
      roles:
        - feast-writer     # created as a ClusterRole
        - feast-reader
  services:
    offlineStore:
      server: {}
    onlineStore:
      server: {}
    registry:
      local:
        server: {}
```

The operator creates `ClusterRole` resources named after each entry in `roles`. Bind them
to subjects using standard Kubernetes `ClusterRoleBinding` or `RoleBinding` resources.

> Kubernetes auth requires all services to be exposed as servers (the controller rejects
> partial configurations where some services are local while RBAC is enabled).

**SDK docs**: [Feast RBAC](../reference/auth/rbac.md)

---

## OIDC authorization

OIDC authorization validates Bearer tokens against an OIDC provider (Keycloak, Dex, etc.).

### Secret format

Create a Secret with the OIDC client credentials:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: oidc-secret
stringData:
  client_id: <your-client-id>
  auth_discovery_url: https://keycloak.example.com/realms/feast/.well-known/openid-configuration
  client_secret: <your-client-secret>
  username: <service-account-username>     # used for client-credentials flow
  password: <service-account-password>
```

Reference the Secret from the CR:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-oidc
spec:
  feastProject: my_project
  authz:
    oidc:
      secretRef:
        name: oidc-secret
```

### Advanced OIDC options

```yaml
authz:
  oidc:
    secretRef:
      name: oidc-secret
    secretKeyName: client_id          # override the default Secret key name
    tokenEnvVar: FEAST_TOKEN          # env var from which servers read the Bearer token
    verifySSL: false                  # disable SSL verification (dev only)
    caCertConfigMap: oidc-ca-cert     # ConfigMap with CA cert for SSL verification
```

**SDK docs**: [Feast OIDC Auth](../reference/auth/oidc.md)

---

## TLS for servers

Each server accepts a `tls` block pointing to a Kubernetes Secret that holds the TLS
certificate and key.

### Creating a TLS Secret

```sh
kubectl create secret tls feast-tls \
  --cert=path/to/tls.crt \
  --key=path/to/tls.key \
  -n <namespace>
```

### Applying TLS to servers

```yaml
services:
  onlineStore:
    server:
      tls:
        secretRef:
          name: feast-tls
  offlineStore:
    server:
      tls:
        secretRef:
          name: feast-tls
  registry:
    local:
      server:
        tls:
          secretRef:
            name: feast-tls
```

Each service can use different TLS Secrets.

### Custom certificate key names

By default the operator looks for keys `tls.crt` and `tls.key`. Override with:

```yaml
tls:
  secretRef:
    name: feast-tls
  certKeyName: server.crt     # default: tls.crt
```

### mTLS — providing a CA certificate

For mutual TLS (client certificate verification), supply a CA cert via a ConfigMap:

```yaml
tls:
  secretRef:
    name: feast-tls
  caCertConfigMapRef:
    name: client-ca-cert
  certKeyName: tls.crt
```

---

## OpenShift non-TLS mode

On OpenShift, services are typically accessed via Routes with TLS termination at the edge.
In this case it is common to run the Feast servers without internal TLS:

```yaml
# See config/samples/v1_featurestore_all_openshift_non_tls.yaml
services:
  onlineStore:
    server: {}    # no tls block
  offlineStore:
    server: {}
  registry:
    local:
      server: {}
```

---

## See also

- [API reference — `AuthzConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#authzconfig)
- [API reference — `TlsConfigs`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#tlsconfigs)
- [Sample: Kubernetes auth](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_kubernetes_auth.yaml)
- [Sample: OIDC auth](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_oidc_auth.yaml)
- [Sample: Postgres with TLS volumes](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_postgres_db_volumes_tls.yaml)
- [Sample: OpenShift non-TLS](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_all_openshift_non_tls.yaml)
- [Feast SDK — Auth Overview](../reference/auth/)
