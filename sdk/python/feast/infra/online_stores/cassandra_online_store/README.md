# Cassandra/Astra DB Online Store

This contribution makes it possible to use [Apache Cassandra™](https://cassandra.apache.org) / 
[Astra DB](https://astra.datastax.com/) as online store for Feast.

Once the database connection and the keyspace are configured, everything else
is handled as with any other online store: table creation,
read/write from/to table and table destruction.

## Quick usage

The following refers to the [Feast quickstart](https://docs.feast.dev/getting-started/quickstart) page. Only
Step 2 ("Create a feature repository") is slightly different, as it involves
a bit of specific configuration about the Astra DB / Cassandra cluster you
are going to use.

It will be assumed that Feast has been installed in your system.

### Creating the feature repository

The easiest way to get started is to use the Feast CLI to initialize a new
feature store. Once Feast is installed, the command

```
feast init FEATURE_STORE_NAME -t cassandra
```

will interactively help you create the `feature_store.yaml` with the
required configuration details to access your Cassandra / Astra DB instance.

Alternatively, you can run `feast init -t FEATURE_STORE_NAME`, as described
in the quickstart, and then manually edit the `online_store` key in
the `feature_store.yaml` file as detailed below.

The following steps (setup of feature definitions, deployment of the store,
generation of training data, materialization, fetching of online/offline
features) proceed exactly as in the general Feast quickstart instructions.

#### Cassandra setup

The only required settings are `hosts` and `type`. The port number
is to be provided only if different than the default (9042),
and username/password only if the database requires authentication.

```yaml
[...]
online_store:
    type: cassandra
    hosts:
        - 192.168.1.1
        - 192.168.1.2
        - 192.168.1.3
    keyspace: KeyspaceName
    port: 9042                                                              # optional
    username: user                                                          # optional
    password: secret                                                        # optional
    protocol_version: 5                                                     # optional
    load_balancing:                                                         # optional
        local_dc: 'datacenter1'                                             # optional
        load_balancing_policy: 'TokenAwarePolicy(DCAwareRoundRobinPolicy)'  # optional
    read_concurrency: 100                                                   # optional
    write_concurrency: 100                                                  # optional
```

#### Astra DB setup:

To point Feast to using an Astra DB instance as online store, an 
[Astra DB token](https://awesome-astra.github.io/docs/pages/astra/create-token/#c-procedure)
with "Database Administrator" role is required: provide the Client ID and
Client Secret in the token as username and password.

The 
["secure connect bundle"](https://awesome-astra.github.io/docs/pages/astra/download-scb/#c-procedure)
for connecting to the database is also needed:
its full path must be given in the configuration below:

```yaml
[...]
online_store:
    type: cassandra
    secure_bundle_path: /path/to/secure/bundle.zip
    keyspace: KeyspaceName
    username: Client_ID
    password: Client_Secret
    protocol_version: 4                                                     # optional
    load_balancing:                                                         # optional
        local_dc: 'eu-central-1'                                            # optional
        load_balancing_policy: 'TokenAwarePolicy(DCAwareRoundRobinPolicy)'  # optional
    read_concurrency: 100                                                   # optional
    write_concurrency: 100                                                  # optional
```

#### Protocol version and load-balancing settings

Whether on Astra DB or Cassandra, there are some optional settings in the
store definition yaml:

```yaml
    [...]
    protocol_version: 5                                                     # optional
    load_balancing:                                                         # optional
        local_dc: 'datacenter1'                                             # optional
        load_balancing_policy: 'TokenAwarePolicy(DCAwareRoundRobinPolicy)'  # optional
```

If you specify a protocol version (4 for `Astra DB` as of June 2022, 5 for `Cassandra 4.*`),
you avoid the drivers having to negotiate it on their own, thus speeding up initialization
time (and reducing the `INFO` messages being logged). See [this page](https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/#cassandra.ProtocolVersion) for a listing
of protocol versions.

You should provide the load-balancing properties as well (the reference datacenter
to use for the connection and the load-balancing policy to use). In a future version
of the driver, according to the warnings issued in the logs, this will become mandatory.
The former parameter is a region name for Astra DB instances (as can be verified on the Astra DB UI).
See the source code of the online store integration for the allowed values of
the latter parameter.

#### Read/write concurrency value

You can optionally specify the value of `read_concurrency` and `write_concurrency`,
which will be passed to the Cassandra driver function handling
[concurrent reading/writing of multiple entities](https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/concurrent/#module-cassandra.concurrent).
Consult the reference for guidance on this parameter (which in most cases can be left to its default value of).
This is relevant only for retrieval of several entities at once and during bulk writes, such as in the materialization step.

### More info

For a more detailed walkthrough, please see the
[Awesome Astra](https://awesome-astra.github.io/docs/pages/tools/integration/feast/)
page on the Feast integration.

## Features

The plugin leverages the architecture of Cassandra for optimal performance:

- table partitioning tailored to data access pattern;
- prepared statements.

#### Credits

The author of this plugin acknowledges prior exploratory work by
[`hamzakpt`](https://github.com/hamzakpt) and Brian Mortimore,
on which this implementation is loosely based.
