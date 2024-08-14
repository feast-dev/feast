# Authorization manager
**TODO** Complete and validate once the code is consolidated

An authorization manager is an implementation of the `AuthManager` interface that is plugged into one of the Feast servers to extract user details from the current request and inject them into the [permissions](../../getting-started/concepts/permissions.md) framework.

{% hint style="info" %}
**Note**: Feast does not provide authentication capabilities; it is the client's responsibility to manage the authentication token and pass it to
the Feast server, which then validates the token and extracts user details from the configured authentication server.
{% endhint %}

Two implementations are provided out-of-the-box:
* The `OidcAuthManager` implementation, using a configurable OIDC server to extract the user details.
* The `KubernetesAuthManager` implementation, using the Kubernetes RBAC resources to extract the user details.

**TODO** Working assumptions for the auth manager implementations (e.g. bearer tokens)
**TODO** Instruct how to configure it in the servers
**TODO** Instruct how to configure it in the helm releases 
