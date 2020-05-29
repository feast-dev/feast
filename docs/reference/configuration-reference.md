# Configuration Reference

## Feast CLI and Feast SDK

Configuration options for both the Feast CLI and Feast Python SDK can be defined in the following locations, in order of precedence:

1. **Command line arguments or initialized arguments:** Passing parameters to the CLI or instantiating the Feast Client object with specific parameters will take precedence above other parameters.
2. **Environmental variables:** Environmental variables can be set to provide configuration options. They must be prefixed with `FEAST_` . For example `FEAST_CORE_URL` .
3. **Configuration file:** Options with the lowest precedence are configured in the Feast configuration file. Feast will look for or create this configuration file in `~/.feast/config` if it does not already exist. All options must be defined in the `[general]` section of this file.

The following list is a subset of all configuration properties. Please see [this link](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/constants.py) for the complete list of configuration options

| Option | Description | Default |
| :--- | :--- | :--- |


| `FEAST_CONFIG` | Location of Feast configuration file | `/.feast/config` |
| :--- | :--- | :--- |


<table>
  <thead>
    <tr>
      <th style="text-align:left"><code>CONFIG_FEAST_ENV_VAR_PREFIX</code>
      </th>
      <th style="text-align:left">
        <p>Default prefix to Feast environmental variable options.</p>
        <p>Does not apply to <code>FEAST_CONFIG</code>
        </p>
      </th>
      <th style="text-align:left"><code>FEAST_</code>
      </th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

| `PROJECT` | Default Feast project to use | `default` |
| :--- | :--- | :--- |


| `CORE_URL` | URL used to connect to Feast Core | `localhost:6565` |
| :--- | :--- | :--- |


| `CORE_ENABLE_SSL` | Enables TLS/SSL on connections to Feast Core | `False` |
| :--- | :--- | :--- |


| `CORE_AUTH_ENABLED` | Enable user authentication when connecting to a Feast Core instance | `False` |
| :--- | :--- | :--- |


| `CORE_AUTH_TOKEN` | Provide a static `JWT` token to authenticate with Feast Core | `Null` |
| :--- | :--- | :--- |


| `CORE_SERVER_SSL_CERT` | Path to certificate\(s\) used by Feast Client to authenticate TLS connection to Feast Core \(not to authenticate you as a client\). | `Null` |
| :--- | :--- | :--- |


| `SERVING_URL` | URL used to connect to Feast Serving | `localhost:6566` |
| :--- | :--- | :--- |


| `SERVING_ENABLE_SSL` | Enables TLS/SSL on connections to Feast Serving | `False` |
| :--- | :--- | :--- |


| `SERVING_SERVER_SSL_CERT` | Path to certificate\(s\) used by Feast Client to authenticate TLS connection to Feast Serving \(not to authenticate you as a client\). | `None` |
| :--- | :--- | :--- |


| `GRPC_CONNECTION_TIMEOUT_DEFAULT` | Default gRPC connection timeout to both Feast Serving and Feast Core \(in seconds\) | `3` |
| :--- | :--- | :--- |


| `GRPC_CONNECTION_TIMEOUT_APPLY` | Default gRPC connection timeout when sending an ApplyFeatureSet command to Feast Core \(in seconds\) | `600` |
| :--- | :--- | :--- |


| `BATCH_FEATURE_REQUEST_WAIT_S` | Time to wait for batch feature requests before timing out. | `600` |
| :--- | :--- | :--- |


### Configuration File

Feast Configuration File \(`~/.feast/config`\)

```text
[general]
project = default
core_url = localhost:6565
```

### Environmental Variables

```bash
FEAST_CORE_URL=my_feast:6565 FEAST_PROJECT=default feast projects list
```

### Feast SDK

```python
client = Client(
    core_url="localhost:6565",
    project="default"
)
```

