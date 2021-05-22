---
description: 'Secure Feast with SSL/TLS, Authentication and Authorization.'
---

# Security

## 1. Overview

![Overview of Feast&apos;s Security Methods.](../.gitbook/assets/untitled-25-1-%20%282%29%20%282%29%20%282%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%281%29%20%284%29.jpg)

Feast supports the following security methods:

* [SSL/TLS on messaging between Feast Core, Feast Serving and Feast SDKs.](security.md#2-ssl-tls)
* [Authentication to Feast Core and Serving based on Open ID Connect ID tokens.](security.md#3-authentication)
* [Authorization based on project membership and delegating authorization grants to external Authorization Server.](security.md#4-authorization)

[Important notes to take note when using Authentication/Authorization](security.md#5-authentication-and-authorization).

## **2. SSL/TLS**

Feast supports SSL/TLS encryption for inter-service communication between Core, Serving and SDKs to be encrypted with SSL/TLS.

### Configuring SSL/TLS on Core/Serving

SSL/TLS can be configured via the following properties in their corresponding `application.yml`files:

| Configuration {Property | Description |
| :--- | :--- |
| `grpc.server.security.enabled` | Enables SSL/TLS functionality if `true` |
| `grpc.server.security.certificateChain` | Provide the path to certificate chain. |
| `grpc.server.security.privateKey` | Provide the to private key. |

> Read more on enabling SSL/TLS in the[ gRPC starter docs.](https://yidongnan.github.io/grpc-spring-boot-starter/en/server/security.html#enable-transport-layer-security)

### Configuring SSL/TLS on Python SDK/CLI

To enable SSL/TLS in the [Feast Python SDK](https://api.docs.feast.dev/python/#feast.client.Client)/CLI, the config options should be set via `feast config`:

| Configuration Option | Description |
| :--- | :--- |
| `core_enable_ssl` | Enables SSL/TLS functionality on connections to Feast core if `true` |
| `serving_enable_ssl` | Enables SSL/TLS functionality on connections to Feast Serving if `true` |
| `core_server_ssl_cert` | Optional. Specifies the path of the root certificate used to verify Core Service's identity. If omitted, will use system certificates. |
| `serving_server_ssl_cert` | Optional. Specifies the path of the root certificate used to verify Serving Service's identity. If omitted, will use system certificates. |

{% hint style="info" %}
The Python SDK automatically uses SSL/TLS when connecting to Feast Core/Serving via port 443.
{% endhint %}

### Configuring SSL/TLS on Go SDK

Configure SSL/TLS on the Go SDK by passing configuration via `SecurityConfig`:

```go
cli, err := feast.NewSecureGrpcClient("localhost", 6566, feast.SecurityConfig{
    EnableTLS: true,
         TLSCertPath: "/path/to/cert.pem",
})Option
```

| Config Option | Description |
| :--- | :--- |
| `EnableTLS` | Enables SSL/TLS functionality when connecting to Feast if `true` |
| `TLSCertPath` | Optional. Provides the path of the root certificate used to verify Feast Service's identity. If omitted, will use system certificates. |

### Configuring SSL/TLS on **Java** SDK

Configure SSL/TLS on the Java SDK by passing configuration via `SecurityConfig`:

```java
FeastClient client = FeastClient.createSecure("localhost", 6566, 
    SecurityConfig.newBuilder()
      .setTLSEnabled(true)
      .setCertificatePath(Optional.of("/path/to/cert.pem"))
      .build());
```

| Config Option | Description |
| :--- | :--- |
| `setTLSEnabled()` | Enables SSL/TLS functionality when connecting to Feast if `true` |
| `setCertificatesPath()` | Optional. Set the path of the root certificate used to verify Feast Service's identity. If omitted, will use system certificates. |

## **3. Authentication**

{% hint style="warning" %}
It is recommended that SSL/TLS be enabled prior to enabling authentication to prevent man in the middle attacks.
{% endhint %}

Authentication can be enabled to authenticate and identify client requests to Feast Core/Serving. Currently, Feast uses[ ](https://auth0.com/docs/protocols/openid-connect-protocol)[Open ID Connect \(OIDC\)](https://auth0.com/docs/protocols/openid-connect-protocol) ID tokens \(ie [Google Open ID Connect](https://developers.google.com/identity/protocols/oauth2/openid-connect)\) to authenticate client requests.

#### Configuring Authentication in Core/Serving

Authentication can be configured for Core/Serving via properties in their corresponding `application.yml`:

| Configuration Property | Description |
| :--- | :--- |
| `feast.security.authentication.enabled` | Enables Authentication functionality if `true`. |
| `feast.security.authentication.provider` | Authentication Provider type. Currently only supports `jwt` |
| `feast.security.authentication.option.jwkEndpointURI` | HTTPS URL used by Feast to retrieved the [JWK](https://tools.ietf.org/html/rfc7517) used verify OIDC ID tokens. |

{% hint style="info" %}
`jwkEndpointURI` is set retrieve Google's OIDC JWK by default, allowing OIDC ID tokens issued by Google to be used for authentication.
{% endhint %}

Behind the scenes, Feast Core/Serving authenticates by:

* Extracting the OIDC ID token `TOKEN`from gRPC metadata submitted with request:

```text
('authorization', 'Bearer: TOKEN')
```

* Validating the token's signature using the JWK retrieved from the `jwkEndpointURI`to ensure token is authentic and produced by the Authentication Provider.

### **Authenticating Serving with Core**

Feast Serving needs to communicate with Core during normal operation. When both authentication and authorization is enabled on Core, Serving is forced to authenticate its requests to Core. Otherwise, Serving will fail to start with an Authentication failure error when connecting to Core.

Properties used to configure Serving authentication via `application.yml`:

| Configuration Property | Description |
| :--- | :--- |
| `feast.core-authentication.enabled` | Indicates to Serving to authenticate when communicating with Feast Core. |
| `feast.core-authentication.provider` | Selects the provider that Serving will use to retrieve credentials that it will use to authenticate with Core. Valid providers are `google` and `oauth`. |

{% tabs %}
{% tab title="Google Provider" %}
Google Provider automatically extracts the credential from the credential JSON file.

* [`GOOGLE_APPLICATION_CREDENTIALS` environment variable](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable) should be set to path of the  credential JSON file. 
{% endtab %}

{% tab title="OAuth Provider" %}
OAuth Provider makes an OAuth [client credentials](https://auth0.com/docs/flows/call-your-api-using-the-client-credentials-flow) request to obtain the credential. It requires the following options to be set at `feast.security.core-authentication.options.`:

| Configuration Property | Description |
| :--- | :--- |


| `oauth_url` | Target URL to make the client credentials request to. |
| :--- | :--- |


| `grant_type` | OAuth grant type. Should be set as `client_credentials` |
| :--- | :--- |


| `client_id` | Client Id used in the client credentials request. |
| :--- | :--- |


| `client_secret` | Client secret used in the client credentials request. |
| :--- | :--- |


<table>
  <thead>
    <tr>
      <th style="text-align:left"><code>audience</code>
      </th>
      <th style="text-align:left">
        <p>Target audience of the credential. Should be set to host URL of Core.</p>
        <p>(ie <code>https://localhost</code> if Core listens on <code>localhost</code>).</p>
      </th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

| `jwkEndpointURI` | HTTPS URL used to retrieve a JWK that can be used to decode the credential. |
| :--- | :--- |
{% endtab %}
{% endtabs %}

### **Enabling Authentication in Python SDK/CLI**

Configure the Feast Python SDK/CLI to use authentication via `feast config`:

```python
$ feast config set enable_auth true
```

| Configuration Option | Description |
| :--- | :--- |
| `enable_auth` | Enables authentication functionality if set to `true`. |
| `auth_provider` | Use an authentication provider to obtain a credential for authentication. Currently supports `google` and `oauth`. |
| `auth_token` | Manually specify an static  token for use in authentication. Overrules `auth_provider` if both are set. |

{% tabs %}
{% tab title="Google Provider" %}
Google Provider automatically finds and use Google Credentials for authenticating requests:

* Google Provider would automatically use user credentials for authenticating requests if user has authenticated with the `gcloud` CLI via:

```text
$ gcloud auth application-default login
```

* Alternatively the Google Provider can be configured to use credentials JSON file via`GOOGLE_APPLICATION_CREDENTIALS` environmental variable \([read more](https://cloud.google.com/docs/authentication/getting-started)\):

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
$ gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
```
{% endtab %}

{% tab title="OAuth Provider" %}
OAuth Provider makes an OAuth [client credentials](https://auth0.com/docs/flows/call-your-api-using-the-client-credentials-flow) request to obtain the credential/token used to authenticate Feast requests. The OAuth provider requires the following config options to be set via `feast config`:

| Configuration Property | Description |
| :--- | :--- |


| `oauth_token_request_url` | Target URL to make the client credentials request to. |
| :--- | :--- |


| `oauth_grant_type` | OAuth grant type. Should be set as `client_credentials` |
| :--- | :--- |


| `oauth_client_id` | Client Id used in the client credentials request. |
| :--- | :--- |


| `oauth_client_secret` | Client secret used in the client credentials request. |
| :--- | :--- |


<table>
  <thead>
    <tr>
      <th style="text-align:left"><code>oauth_audience</code>
      </th>
      <th style="text-align:left">
        <p>Target audience of the credential. Should be set to host URL of target
          Service.</p>
        <p>(ie <code>https://localhost</code> if Service listens on <code>localhost</code>).</p>
      </th>
    </tr>
  </thead>
  <tbody></tbody>
</table>
{% endtab %}
{% endtabs %}

### **Enabling Authentication in Go SDK**

Configure the Feast Java SDK to use authentication by specifying credential via `SecurityConfig`:

```go
// error handling omitted.
// Use Google Credential as provider.
cred, _ := feast.NewGoogleCredential("localhost:6566")
cli, _ := feast.NewSecureGrpcClient("localhost", 6566, feast.SecurityConfig{
  // Specify the credential to provide tokens for Feast Authentication.  
    Credential: cred, 
})
```

{% tabs %}
{% tab title="Google Credential" %}
Google Credential uses Service Account credentials JSON file set via`GOOGLE_APPLICATION_CREDENTIALS` environmental variable \([read more](https://cloud.google.com/docs/authentication/getting-started)\) to obtain tokens for Authenticating Feast requests:

* Exporting `GOOGLE_APPLICATION_CREDENTIALS`

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
```

* Create Google Credential with target audience. 

```go
cred, _ := feast.NewGoogleCredential("localhost:6566")
```

> Target audience of the credential should be set to host URL of target Service. \(ie `https://localhost` if Service listens on `localhost`\):
{% endtab %}

{% tab title="OAuth Credential" %}
OAuth Credential makes an OAuth [client credentials](https://auth0.com/docs/flows/call-your-api-using-the-client-credentials-flow) request to obtain the credential/token used to authenticate Feast requests:

* Create OAuth Credential with parameters:

```go
cred := feast.NewOAuthCredential("localhost:6566", "client_id", "secret", "https://oauth.endpoint/auth")
```

| Parameter | Description |
| :--- | :--- |


<table>
  <thead>
    <tr>
      <th style="text-align:left"><code>audience</code>
      </th>
      <th style="text-align:left">
        <p>Target audience of the credential. Should be set to host URL of target
          Service.</p>
        <p>(ie <code>https://localhost</code> if Service listens on <code>localhost</code>).</p>
      </th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

| `clientId` | Client Id used in the client credentials request. |
| :--- | :--- |


| `clientSecret` | Client secret used in the client credentials request. |
| :--- | :--- |


| `endpointURL` | Target URL to make the client credentials request to. |
| :--- | :--- |
{% endtab %}
{% endtabs %}

### **Enabling Authentication in Java SDK**

Configure the Feast Java SDK to use authentication by setting credentials via `SecurityConfig`:

```java
// Use GoogleAuthCredential as provider.
CallCredentials credentials = new GoogleAuthCredentials(
    Map.of("audience", "localhost:6566"));

FeastClient client = FeastClient.createSecure("localhost", 6566, 
    SecurityConfig.newBuilder()
      // Specify the credentials to provide tokens for Feast Authentication.  
      .setCredentials(Optional.of(creds))
      .build());
```

{% tabs %}
{% tab title="GoogleAuthCredentials" %}
GoogleAuthCredentials uses Service Account credentials JSON file set via`GOOGLE_APPLICATION_CREDENTIALS` environmental variable \([read more](https://cloud.google.com/docs/authentication/getting-started)\) to obtain tokens for Authenticating Feast requests:

* Exporting `GOOGLE_APPLICATION_CREDENTIALS`

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
```

* Create Google Credential with target audience. 

```java
CallCredentials credentials = new GoogleAuthCredentials(
    Map.of("audience", "localhost:6566"));
```

> Target audience of the credentials should be set to host URL of target Service. \(ie `https://localhost` if Service listens on `localhost`\):
{% endtab %}

{% tab title="OAuthCredentials" %}
OAuthCredentials makes an OAuth [client credentials](https://auth0.com/docs/flows/call-your-api-using-the-client-credentials-flow) request to obtain the credential/token used to authenticate Feast requests:

* Create OAuthCredentials with parameters:

```java
CallCredentials credentials = new OAuthCredentials(Map.of(
  "audience": "localhost:6566",
  "grant_type", "client_credentials",
  "client_id", "some_id",
  "client_id", "secret",
  "oauth_url", "https://oauth.endpoint/auth",
  "jwkEndpointURI", "https://jwk.endpoint/jwk"));
```

| Parameter | Description |
| :--- | :--- |


<table>
  <thead>
    <tr>
      <th style="text-align:left"><code>audience</code>
      </th>
      <th style="text-align:left">
        <p>Target audience of the credential. Should be set to host URL of target
          Service.</p>
        <p>(ie <code>https://localhost</code> if Service listens on <code>localhost</code>).</p>
      </th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

| `grant_type` | OAuth grant type. Should be set as `client_credentials` |
| :--- | :--- |


| `client_id` | Client Id used in the client credentials request. |
| :--- | :--- |


| `client_secret` | Client secret used in the client credentials request. |
| :--- | :--- |


| `oauth_url` | Target URL to make the client credentials request to obtain credential. |
| :--- | :--- |


| `jwkEndpointURI` | HTTPS URL used to retrieve a JWK that can be used to decode the credential. |
| :--- | :--- |
{% endtab %}
{% endtabs %}

## 4. Authorization

{% hint style="info" %}
Authorization requires authentication to be configured in order to obtain user identity to use for authorizing requests.
{% endhint %}

Authorization provides access control to FeatureSets/Features based on project membership. Users that are members of a project are authorized to:

* Create/Update a Feature Set in the Project.
* Retrieve Feature Values for Features in that Project.

### **Authorization API/Server**

![Feast Authorization Flow](../.gitbook/assets/rsz_untitled23%20%282%29%20%282%29%20%282%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%284%29.jpg)

Feast delegates Authorization grants to a external Authorization Server that implements the [Authorization Open API specification](https://github.com/feast-dev/feast/blob/v0.7-branch/common/src/main/resources/api.yaml).

* Feast checks whether a user is authorized to make a request by making a `checkAccessRequest` to the Authorization Server.
* The Authorization Server should return a `AuthorizationResult` with whether user is allowed to make the request.

Authorization can be configured for Core/Serving via properties in their corresponding `application.yml`

| Configuration Property | Description |
| :--- | :--- |
| `feast.security.authorization.enabled` | Enables authorization functionality if `true`. |
| `feast.security.authorization.provider` | Authentication Provider type. Currently only supports `http` |
| `feast.security.authorization.option.authorizationUrl` | URL endpoint of Authorization Server to make check access requests to. |
| `feast.security.authorization.option.subjectClaim` | Optional. Name of the claim of the to extract from the ID Token to include in the check access request as Subject. |

{% hint style="info" %}
Example of [Authorization Server with Keto](https://github.com/feast-dev/feast-keto-auth-server) can used as a reference implementation for implementing an Authorization Server that Feast supports.
{% endhint %}

## **5. Authentication & Authorization**

Things to note when using Authentication & Authorization:

* Enabling Authentication without Authorization makes authentication **optional**. Users can still make requests unauthenticated.
* Enabling Authorization forces all requests made to be authenticated. Requests that are not authenticated are **dropped.**

