# Authorization Manager
An Authorization Manager is an instance of the `AuthManager` class that is plugged into one of the Feast servers to extract user details from the current request and inject them into the [permissions](../../getting-started/concepts/permissions.md) framework.

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

The server, in turn, uses the same OIDC server to validate the token and extract the user roles from the token itself.

Some assumptions are made in the OIDC server configuration:
* The OIDC token refers to a client with roles matching the RBAC roles of the configured `Permission`s (*)
* The roles are exposed in the access token passed to the server

(*) Please note that **the role match is case-sensitive**, e.g. the name of the role in the OIDC server and in the `Permission` configuration
must be exactly the same.

For example, the access token for a client `app` of a user with `reader` role should have the following `resource_access` section:
```json
{
  "resource_access": {
    "app": {
      "roles": [
        "reader"
      ]
    },
}
```

An example of OIDC authorization configuration is the following: 
```yaml
project: my-project
auth:
  type: oidc
  client_id: _CLIENT_ID__
  client_secret: _CLIENT_SECRET__
  realm: _REALM__
  auth_server_url: _OIDC_SERVER_URL_
  auth_discovery_url: _OIDC_SERVER_URL_/realms/master/.well-known/openid-configuration
...
```

In case of client configuration, the following settings must be added to specify the current user:
```yaml
auth:
  ...
  username: _USERNAME_
  password: _PASSWORD_
```

### Kubernetes RBAC Authorization
With Kubernetes RBAC Authorization, the client uses the service account token as the authorizarion bearer token, and the
server fetches the associated roles from the Kubernetes RBAC resources.

An example of Kubernetes RBAC authorization configuration is the following: 
{% hint style="info" %}
**NOTE**: This configuration will only work if you deploy feast on Openshift or a Kubernetes platform.
{% endhint %}
```yaml
project: my-project
auth:
  type: kubernetes
...
```

In case the client cannot run on the same cluster as the servers, the client token can be injected using the `LOCAL_K8S_TOKEN` 
environment variable on the client side. The value must refer to the token of a service account created on the servers cluster
and linked to the desired RBAC roles.