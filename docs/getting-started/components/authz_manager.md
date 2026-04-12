# Authorization Manager
An Authorization Manager is an instance of the `AuthManager` class that is plugged into one of the Feast servers to extract user details from the current request and inject them into the [permission](../../getting-started/concepts/permission.md) framework.

{% hint style="info" %}
**Note**: Feast does not provide authentication capabilities; it is the client's responsibility to manage the authentication token and pass it to
the Feast server, which then validates the token and extracts user details from the configured authentication server.
{% endhint %}

Two authorization managers are supported out-of-the-box:
* One using a configurable OIDC server to extract the user details.
* One using the Kubernetes RBAC resources to extract the user details.

These instances are created when the Feast servers are initialized, according to the authorization configuration defined in
their own `feature_store.yaml`.

Feast servers and clients must have consistent authorization configuration, so that the client proxies can automatically inject
the authorization tokens that the server can properly identify and use to enforce permission validations.


## Design notes
The server-side implementation of the authorization functionality is defined [here](./../../../sdk/python/feast/permissions/server).
Few of the key models, classes to understand the authorization implementation on the client side can be found [here](./../../../sdk/python/feast/permissions/client).

## Configuring Authorization
The authorization is configured using a dedicated `auth` section in the `feature_store.yaml` configuration.

**Note**: As a consequence, when deploying the Feast servers with the Helm [charts](../../../infra/charts/feast-feature-server/README.md),
the `feature_store_yaml_base64` value must include the `auth` section to specify the authorization configuration.

### No Authorization
This configuration applies the default `no_auth` authorization:
```yaml
project: my-project
auth:
  type: no_auth
...
```

### OIDC Authorization
With OIDC authorization, the Feast client proxies retrieve the JWT token from an OIDC server (or [Identity Provider](https://openid.net/developers/how-connect-works/))
and append it in every request to a Feast server, using an [Authorization Bearer Token](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#bearer).

The server, in turn, uses the same OIDC server to validate the token and extract user details — including username, roles, and groups — from the token itself.

Some assumptions are made in the OIDC server configuration:
* The OIDC token refers to a client with roles matching the RBAC roles of the configured `Permission`s (*)
* The roles are exposed in the access token under `resource_access.<client_id>.roles`
* The JWT token is expected to have a verified signature and not be expired. The Feast OIDC token parser logic validates for `verify_signature` and `verify_exp` so make sure that the given OIDC provider is configured to meet these requirements.
* The `preferred_username` should be part of the JWT token claim.
* For `GroupBasedPolicy` support, the `groups` claim should be present in the access token (requires a "Group Membership" protocol mapper in Keycloak).

(*) Please note that **the role match is case-sensitive**, e.g. the name of the role in the OIDC server and in the `Permission` configuration
must be exactly the same.

For example, the access token for a client `app` of a user with `reader` role and membership in the `data-team` group should have the following claims:
```json
{
  "preferred_username": "alice",
  "resource_access": {
    "app": {
      "roles": [
        "reader"
      ]
    }
  },
  "groups": [
    "data-team"
  ]
}
```

#### Server-Side Configuration

The server requires `auth_discovery_url` and `client_id` to validate incoming JWT tokens via JWKS:
```yaml
project: my-project
auth:
  type: oidc
  client_id: _CLIENT_ID_
  auth_discovery_url: _OIDC_SERVER_URL_/realms/master/.well-known/openid-configuration
...
```

When the OIDC provider uses a self-signed or untrusted TLS certificate (e.g. internal Keycloak on OpenShift), set `verify_ssl` to `false` to disable certificate verification:
```yaml
auth:
  type: oidc
  client_id: _CLIENT_ID_
  auth_discovery_url: https://keycloak.internal/realms/master/.well-known/openid-configuration
  verify_ssl: false
```

{% hint style="warning" %}
Setting `verify_ssl: false` disables TLS certificate verification for all OIDC provider communication (discovery, JWKS, token endpoint). Only use this in development or internal environments where you accept the security risk.
{% endhint %}

#### Client-Side Configuration

The client supports multiple token source modes. The SDK resolves tokens in the following priority order:

1. **Intra-communication token** — internal server-to-server calls (via `INTRA_COMMUNICATION_BASE64` env var)
2. **`token`** — a static JWT string provided directly in the configuration
3. **`token_env_var`** — the name of an environment variable containing the JWT
4. **`client_secret`** — fetches a token from the OIDC provider using client credentials or ROPC flow (requires `auth_discovery_url` and `client_id`)
5. **`FEAST_OIDC_TOKEN`** — default fallback environment variable
6. **Kubernetes service account token** — read from `/var/run/secrets/kubernetes.io/serviceaccount/token` when running inside a pod

**Token passthrough** (for use with external token providers like [kube-authkit](https://github.com/opendatahub-io/kube-authkit)):
```yaml
project: my-project
auth:
  type: oidc
  token_env_var: FEAST_OIDC_TOKEN
```

Or with a bare `type: oidc` (no other fields) — the SDK falls back to the `FEAST_OIDC_TOKEN` environment variable or a mounted Kubernetes service account token:
```yaml
project: my-project
auth:
  type: oidc
```

**Client credentials / ROPC flow** (existing behavior, unchanged):
```yaml
project: my-project
auth:
  type: oidc
  client_id: test_client_id
  client_secret: test_client_secret
  username: test_user_name
  password: test_password
  auth_discovery_url: http://localhost:8080/realms/master/.well-known/openid-configuration
```

When using client credentials or ROPC flows, the `verify_ssl` setting also applies to the discovery and token endpoint requests.

#### Multi-Token Support (OIDC + Kubernetes Service Account)

When the Feast server is configured with OIDC auth and deployed on Kubernetes, the `OidcTokenParser` can handle both Keycloak JWT tokens and Kubernetes service account tokens. Incoming tokens that contain a `kubernetes.io` claim are validated via the Kubernetes Token Access Review API and the namespace is extracted from the authenticated identity — no RBAC queries are performed, so the server service account only needs `tokenreviews/create` permission. All other tokens follow the standard OIDC/Keycloak JWKS validation path. This enables `NamespaceBasedPolicy` enforcement for service account tokens while using `GroupBasedPolicy` and `RoleBasedPolicy` for OIDC user tokens.

### Kubernetes RBAC Authorization
With Kubernetes RBAC Authorization, the client uses the service account token as the authorizarion bearer token, and the
server fetches the associated roles from the Kubernetes RBAC resources. Feast supports advanced authorization by extracting user groups and namespaces from Kubernetes tokens, enabling fine-grained access control beyond simple role matching. This is achieved by leveraging Kubernetes Token Access Review, which allows Feast to determine the groups and namespaces associated with a user or service account.

An example of Kubernetes RBAC authorization configuration is the following: 
{% hint style="info" %}
**NOTE**: This configuration will only work if you deploy feast on Openshift or a Kubernetes platform.
{% endhint %}
```yaml
project: my-project
auth:
  type: kubernetes
  user_token: <user_token> #Optional, else service account token Or env var is used for getting the token
...
```

In case the client cannot run on the same cluster as the servers, the client token can be injected using the `LOCAL_K8S_TOKEN` 
environment variable on the client side. The value must refer to the token of a service account created on the servers cluster
and linked to the desired RBAC roles/groups/namespaces.

More details can be found in [Setting up kubernetes doc](../../reference/auth/kubernetes_auth_setup.md)
