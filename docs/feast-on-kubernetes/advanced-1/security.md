---
description: 'Secure Feast with SSL/TLS, Authentication and Authorization.'
---

# Security

{% hint style="warning" %}
This page applies to Feast 0.7. The content may be out of date for Feast 0.8+
{% endhint %}

### Overview

![Overview of Feast&apos;s Security Methods.](../../.gitbook/assets/untitled-25-1-%20%282%29%20%282%29%20%282%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%281%29%20%284%29.jpg)

Feast supports the following security methods:

* [SSL/TLS on messaging between Feast Core, Feast Online Serving and Feast SDKs.](security.md#2-ssl-tls)
* [Authentication to Feast Core and Serving based on Open ID Connect ID tokens.](security.md#3-authentication)
* [Authorization based on project membership and delegating authorization grants to external Authorization Server.](security.md#4-authorization)

[Important considerations when integrating Authentication/Authorization](security.md#5-authentication-and-authorization).

### **SSL/TLS**

Feast supports SSL/TLS encrypted inter-service communication among Feast Core, Feast Online Serving, and Feast SDKs.

#### Configuring SSL/TLS on Feast Core and Feast Serving

The following properties configure SSL/TLS. These properties are located in their corresponding `application.yml`files:

| Configuration Property | Description |
| :--- | :--- |
| `grpc.server.security.enabled` | Enables SSL/TLS functionality if `true` |
| `grpc.server.security.certificateChain` | Provide the path to certificate chain. |
| `grpc.server.security.privateKey` | Provide the to private key.  |

> Read more on enabling SSL/TLS in the[ gRPC starter docs.](https://yidongnan.github.io/grpc-spring-boot-starter/en/server/security.html#enable-transport-layer-security)

#### Configuring SSL/TLS on Python SDK/CLI

To enable SSL/TLS in the [Feast Python SDK](https://api.docs.feast.dev/python/#feast.client.Client) or [Feast CLI](../getting-started/connect-to-feast/feast-cli.md), set the config options via `feast config`:

| Configuration Option | Description |
| :--- | :--- |
| `core_enable_ssl` | Enables SSL/TLS functionality on connections to Feast core if `true` |
| `serving_enable_ssl` | Enables SSL/TLS functionality on connections to Feast Online Serving if `true` |
| `core_server_ssl_cert` | Optional. Specifies the path of the root certificate used to verify Core Service's identity. If omitted, uses system certificates. |
| `serving_server_ssl_cert` | Optional. Specifies the path of the root certificate used to verify Serving Service's identity. If omitted, uses system certificates. |

{% hint style="info" %}
The Python SDK automatically uses SSL/TLS when connecting to Feast Core and Feast Online Serving via port 443.
{% endhint %}

#### Configuring SSL/TLS on Go SDK

Configure SSL/TLS on the [Go SDK](https://godoc.org/github.com/feast-dev/feast/sdk/go) by passing configuration via `SecurityConfig`:

```go
cli, err := feast.NewSecureGrpcClient("localhost", 6566, feast.SecurityConfig{
    EnableTLS: true,
 		TLSCertPath: "/path/to/cert.pem",
})Option
```

| Config Option | Description |
| :--- | :--- |
| `EnableTLS` | Enables SSL/TLS functionality when connecting to Feast if `true` |
| `TLSCertPath` | Optional. Provides the path of the root certificate used to verify Feast Service's identity. If omitted, uses system certificates. |

#### Configuring SSL/TLS on **Java** SDK

Configure SSL/TLS on the [Feast Java SDK](https://javadoc.io/doc/dev.feast/feast-sdk) by passing configuration via `SecurityConfig`:

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
| `setCertificatesPath()` | Optional. Set the path of the root certificate used to verify Feast Service's identity. If omitted, uses system certificates. |

### **Authentication**

{% hint style="warning" %}
To prevent man in the middle attacks, we recommend that SSL/TLS be implemented prior to authentication.
{% endhint %}

Authentication can be implemented to identify and validate client requests to Feast Core and Feast Online Serving. Currently, Feast uses[ ](https://auth0.com/docs/protocols/openid-connect-protocol)[Open ID Connect \(OIDC\)](https://auth0.com/docs/protocols/openid-connect-protocol) ID tokens \(i.e.  [Google Open ID Connect](https://developers.google.com/identity/protocols/oauth2/openid-connect)\) to authenticate client requests. 

#### Configuring Authentication in Feast Core and Feast Online Serving

Authentication can be configured for Feast Core and Feast Online Serving via properties in their corresponding `application.yml` files:

| Configuration Property | Description |
| :--- | :--- |
| `feast.security.authentication.enabled` | Enables Authentication functionality if `true` |
| `feast.security.authentication.provider` | Authentication Provider type. Currently only supports `jwt` |
| `feast.security.authentication.option.jwkEndpointURI` | HTTPS URL used by Feast to retrieved the [JWK](https://tools.ietf.org/html/rfc7517) used to verify OIDC ID tokens. |

{% hint style="info" %}
`jwkEndpointURI`is set to retrieve Google's OIDC JWK by default, allowing OIDC ID tokens issued by Google to be used for authentication.
{% endhint %}

Behind the scenes, Feast Core and Feast Online Serving authenticate by:

* Extracting the OIDC ID token `TOKEN`from gRPC metadata submitted with request:

```text
('authorization', 'Bearer: TOKEN')
```

* Validates token's authenticity using the JWK retrieved from the `jwkEndpointURI`

#### **Authenticating Serving with Feast Core**

Feast Online Serving communicates with Feast Core during normal operation. When both authentication and authorization are enabled on Feast Core, Feast Online Serving is forced to authenticate its requests to Feast Core. Otherwise, Feast Online Serving produces an Authentication failure error when connecting to Feast Core. 

 Properties used to configure Serving authentication via `application.yml`:

| Configuration Property | Description |
| :--- | :--- |
| `feast.core-authentication.enabled` | Requires Feast Online Serving to authenticate when communicating with Feast Core. |
| `feast.core-authentication.provider`  | Selects provider Feast Online Serving uses to retrieve credentials then used to authenticate requests to Feast Core. Valid providers are `google` and `oauth`. |

{% tabs %}
{% tab title="Google Provider" %}
Google Provider automatically extracts the credential from the credential JSON file.

* Set [`GOOGLE_APPLICATION_CREDENTIALS` environment variable](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable) to the path of the credential in the JSON file. 
{% endtab %}

{% tab title="OAuth Provider" %}
OAuth Provider makes an OAuth [client credentials](https://auth0.com/docs/flows/call-your-api-using-the-client-credentials-flow) request to obtain the credential. OAuth requires the following options to be set at `feast.security.core-authentication.options.`:

<table>
  <thead>
    <tr>
      <th style="text-align:left">Configuration Property</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><code>oauth_url</code>
      </td>
      <td style="text-align:left">Target URL receiving the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>grant_type</code>
      </td>
      <td style="text-align:left">OAuth grant type. Set as <code>client_credentials</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>client_id</code>
      </td>
      <td style="text-align:left">Client Id used in the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>client_secret</code>
      </td>
      <td style="text-align:left">Client secret used in the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>audience</code>
      </td>
      <td style="text-align:left">
        <p>Target audience of the credential. Set to host URL of Feast Core.</p>
        <p>(i.e. <code>https://localhost</code> if Feast Core listens on <code>localhost</code>).</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>jwkEndpointURI</code>
      </td>
      <td style="text-align:left">HTTPS URL used to retrieve a JWK that can be used to decode the credential.</td>
    </tr>
  </tbody>
</table>
{% endtab %}
{% endtabs %}

#### **Enabling Authentication in Python SDK/CLI**

Configure the [Feast Python SDK](https://api.docs.feast.dev/python/) and [Feast CLI](../getting-started/connect-to-feast/feast-cli.md) to use authentication via `feast config`:

```python
$ feast config set enable_auth true
```

| Configuration Option | Description |
| :--- | :--- |
| `enable_auth` | Enables authentication functionality if set to `true`. |
| `auth_provider` | Use an authentication provider to obtain a credential for authentication. Currently supports `google` and `oauth`.  |
| `auth_token` | Manually specify a static token for use in authentication. Overrules `auth_provider` if both are set. |

{% tabs %}
{% tab title="Google Provider" %}
Google Provider automatically finds and uses Google Credentials to authenticate requests:

* Google Provider automatically uses established credentials for authenticating requests if you are already authenticated with the `gcloud` CLI via:

```text
$ gcloud auth application-default login
```

* Alternatively Google Provider can be configured to use the credentials in the JSON file via`GOOGLE_APPLICATION_CREDENTIALS` environmental variable \([Google Cloud Authentication documentation](https://cloud.google.com/docs/authentication/getting-started)\):

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
```
{% endtab %}

{% tab title="OAuth Provider" %}
OAuth Provider makes an OAuth [client credentials](https://auth0.com/docs/flows/call-your-api-using-the-client-credentials-flow) request to obtain the credential/token used to authenticate Feast requests. The OAuth provider requires the following config options to be set via `feast config`:

<table>
  <thead>
    <tr>
      <th style="text-align:left">Configuration Property</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><code>oauth_token_request_url</code>
      </td>
      <td style="text-align:left">Target URL receiving the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>oauth_grant_type</code>
      </td>
      <td style="text-align:left">OAuth grant type. Set as <code>client_credentials</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>oauth_client_id</code>
      </td>
      <td style="text-align:left">Client Id used in the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>oauth_client_secret</code>
      </td>
      <td style="text-align:left">Client secret used in the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>oauth_audience</code>
      </td>
      <td style="text-align:left">
        <p>Target audience of the credential. Set to host URL of target Service.</p>
        <p>(<code>https://localhost</code> if Service listens on <code>localhost</code>).</p>
      </td>
    </tr>
  </tbody>
</table>
{% endtab %}
{% endtabs %}

#### **Enabling Authentication in Go SDK**

Configure the [Feast Java SDK](https://javadoc.io/doc/dev.feast/feast-sdk/latest/com/gojek/feast/package-summary.html) to use authentication by specifying the credential via `SecurityConfig`:

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
Google Credential uses Service Account credentials JSON file set via`GOOGLE_APPLICATION_CREDENTIALS` environmental variable \([Google Cloud Authentication documentation](https://cloud.google.com/docs/authentication/getting-started)\) to obtain tokens for Authenticating Feast requests:

* Exporting `GOOGLE_APPLICATION_CREDENTIALS`

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
```

* Create a Google Credential with target audience. 

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

<table>
  <thead>
    <tr>
      <th style="text-align:left">Parameter</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><code>audience</code>
      </td>
      <td style="text-align:left">
        <p>Target audience of the credential. Set to host URL of target Service.</p>
        <p>( <code>https://localhost</code> if Service listens on <code>localhost</code>).</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>clientId</code>
      </td>
      <td style="text-align:left">Client Id used in the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>clientSecret</code>
      </td>
      <td style="text-align:left">Client secret used in the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>endpointURL</code>
      </td>
      <td style="text-align:left">Target URL to make the client-credentials request to.</td>
    </tr>
  </tbody>
</table>
{% endtab %}
{% endtabs %}

#### **Enabling Authentication in Java SDK**

Configure the [Feast Java SDK](https://javadoc.io/doc/dev.feast/feast-sdk/latest/com/gojek/feast/package-summary.html) to use authentication by setting credentials via `SecurityConfig`:

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
GoogleAuthCredentials uses Service Account credentials JSON file set via`GOOGLE_APPLICATION_CREDENTIALS` environmental variable \([Google Cloud authentication documentation](https://cloud.google.com/docs/authentication/getting-started)\) to obtain tokens for Authenticating Feast requests:

* Exporting `GOOGLE_APPLICATION_CREDENTIALS`

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
```

* Create a Google Credential with target audience. 

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

<table>
  <thead>
    <tr>
      <th style="text-align:left">Parameter</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><code>audience</code>
      </td>
      <td style="text-align:left">
        <p>Target audience of the credential. Set to host URL of target Service.</p>
        <p>( <code>https://localhost</code> if Service listens on <code>localhost</code>).</p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>grant_type</code>
      </td>
      <td style="text-align:left">OAuth grant type. Set as <code>client_credentials</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>client_id</code>
      </td>
      <td style="text-align:left">Client Id used in the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>client_secret</code>
      </td>
      <td style="text-align:left">Client secret used in the client-credentials request.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>oauth_url</code>
      </td>
      <td style="text-align:left">Target URL to make the client-credentials request to obtain credential.</td>
    </tr>
    <tr>
      <td style="text-align:left"><code>jwkEndpointURI</code>
      </td>
      <td style="text-align:left">HTTPS URL used to retrieve a JWK that can be used to decode the credential.</td>
    </tr>
  </tbody>
</table>
{% endtab %}
{% endtabs %}

### Authorization

{% hint style="info" %}
Authorization requires that authentication be configured to obtain a user identity for use in authorizing requests.
{% endhint %}

Authorization provides access control to FeatureTables and/or Features based on project membership. Users who are members of a project are authorized to:

* Create and/or Update a Feature Table in the Project.
* Retrieve Feature Values for Features in that Project.

#### **Authorization API/Server**

![Feast Authorization Flow](../../.gitbook/assets/rsz_untitled23%20%282%29%20%282%29%20%282%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29%20%283%29.jpg)

Feast delegates Authorization grants to an external Authorization Server that implements the [Authorization Open API specification](https://github.com/feast-dev/feast/blob/master/common/src/main/resources/api.yaml).

* Feast checks whether a user is authorized to make a request by making a `checkAccessRequest` to the Authorization Server.
* The Authorization Server should return a `AuthorizationResult` with whether the user is allowed to make the request.

Authorization can be configured for Feast Core and Feast Online Serving via properties in their corresponding `application.yml`

| Configuration Property | Description |
| :--- | :--- |
| `feast.security.authorization.enabled` | Enables authorization functionality if `true`. |
| `feast.security.authorization.provider` | Authentication Provider type. Currently only supports `http` |
| `feast.security.authorization.option.authorizationUrl` | URL endpoint of Authorization Server to make check access requests to. |
| `feast.security.authorization.option.subjectClaim` | Optional. Name of the claim of the to extract from the ID Token to include in the check access request as Subject. |

{% hint style="info" %}
This example of the [Authorization Server with Keto](https://github.com/feast-dev/feast-keto-auth-server) can be used as a reference implementation for implementing an Authorization Server that Feast supports.
{% endhint %}

### **Authentication & Authorization**

When using Authentication & Authorization, consider:

* Enabling Authentication without Authorization makes authentication **optional**. You can still send unauthenticated requests.
* Enabling Authorization forces all requests to be authenticated. Requests that are not authenticated are **dropped.**



