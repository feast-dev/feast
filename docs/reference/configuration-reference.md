# Configuration Reference

## Feast CLI and Feast SDK

Configuration options for both the Feast CLI and Feast Python SDK can be defined in the following locations, in order of precedence:

1. **Command line arguments or initialized arguments:** Passing parameters to the CLI or instantiating the Feast Client object with specific parameters will take precedence above other parameters.
2. **Environmental variables:** Environmental variables can be set to provide configuration options. They must be prefixed with `FEAST_` . For example `FEAST_CORE_URL` .
3. **Configuration file:** Options with the lowest precedence are configured in the Feast configuration file. Feast will look for or create this configuration file in `~/.feast/config` if it does not already exist. All options must be defined in the `[general]` section of this file.

The following list is a subset of all configuration properties. Please see [this link](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/constants.py) for the complete list of configuration options

<table>
  <thead>
    <tr>
      <th style="text-align:left">Option</th>
      <th style="text-align:left">Description</th>
      <th style="text-align:left">Default</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><code>FEAST_CONFIG</code>
      </td>
      <td style="text-align:left">Location of Feast configuration file</td>
      <td style="text-align:left"><code>/.feast/config</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>CONFIG_FEAST_ENV_VAR_PREFIX</code>
      </td>
      <td style="text-align:left">
        <p>Default prefix to Feast environmental variable options.</p>
        <p>Does not apply to <code>FEAST_CONFIG</code>
        </p>
      </td>
      <td style="text-align:left"><code>FEAST_</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>PROJECT</code>
      </td>
      <td style="text-align:left">Default Feast project to use</td>
      <td style="text-align:left"><code>default</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>CORE_URL</code>
      </td>
      <td style="text-align:left">URL used to connect to Feast Core</td>
      <td style="text-align:left"><code>localhost:6565</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>CORE_ENABLE_SSL</code>
      </td>
      <td style="text-align:left">Enables TLS/SSL on connections to Feast Core</td>
      <td style="text-align:left"><code>False</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>CORE_AUTH_ENABLED</code>
      </td>
      <td style="text-align:left">Enable user authentication when connecting to a Feast Core instance</td>
      <td
      style="text-align:left"><code>False</code>
        </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>CORE_AUTH_TOKEN</code>
      </td>
      <td style="text-align:left">Provide a static <code>JWT</code> token to authenticate with Feast Core</td>
      <td
      style="text-align:left"><code>Null</code>
        </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>CORE_SERVER_SSL_CERT</code>
      </td>
      <td style="text-align:left">Path to certificate(s) used by Feast Client to authenticate TLS connection
        to Feast Core (not to authenticate you as a client).</td>
      <td style="text-align:left"><code>Null</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>SERVING_URL</code>
      </td>
      <td style="text-align:left">URL used to connect to Feast Serving</td>
      <td style="text-align:left"><code>localhost:6566</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>SERVING_ENABLE_SSL</code>
      </td>
      <td style="text-align:left">Enables TLS/SSL on connections to Feast Serving</td>
      <td style="text-align:left"><code>False</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>SERVING_SERVER_SSL_CERT</code>
      </td>
      <td style="text-align:left">Path to certificate(s) used by Feast Client to authenticate TLS connection
        to Feast Serving (not to authenticate you as a client).</td>
      <td style="text-align:left"><code>None</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>GRPC_CONNECTION_TIMEOUT_DEFAULT</code>
      </td>
      <td style="text-align:left">Default gRPC connection timeout to both Feast Serving and Feast Core (in
        seconds)</td>
      <td style="text-align:left"><code>3</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>GRPC_CONNECTION_TIMEOUT_APPLY</code>
      </td>
      <td style="text-align:left">Default gRPC connection timeout when sending an ApplyFeatureSet command
        to Feast Core (in seconds)</td>
      <td style="text-align:left"><code>600</code>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><code>BATCH_FEATURE_REQUEST_WAIT_S</code>
      </td>
      <td style="text-align:left">Time to wait for batch feature requests before timing out.</td>
      <td style="text-align:left"><code>600</code>
      </td>
    </tr>
  </tbody>
</table>

### Usage

#### Configuration File

Feast Configuration File \(`~/.feast/config`\)

```text
[general]
project = default
core_url = localhost:6565
```

#### Environmental Variables

```bash
FEAST_CORE_URL=my_feast:6565 FEAST_PROJECT=default feast projects list
```

#### Feast SDK

```python
client = Client(
    core_url="localhost:6565",
    project="default"
)
```

