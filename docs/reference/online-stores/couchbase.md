# Couchbase Online Store
> NOTE:
> This is a community-contributed online store that is in alpha development. It is not officially supported by the Feast project.

## Description
The [Couchbase](https://www.couchbase.com/) online store provides support for materializing feature values into a Couchbase Operational cluster for serving online features in real-time.

* Only the latest feature values are persisted
* Features are stored in a document-oriented format

The data model for using Couchbase as an online store follows a document format:
* Document ID: `{project}:{table_name}:{entity_key_hex}:{feature_name}`
* Document Content:
    * `metadata`:
        * `event_ts` (ISO formatted timestamp)
        * `created_ts` (ISO formatted timestamp)
        * `feature_name` (String)
    * `value` (Base64 encoded protobuf binary)


## Getting started
In order to use this online store, you'll need to run `pip install 'feast[couchbase]'`. You can then get started with the command `feast init REPO_NAME -t couchbase`.

To get started with Couchbase Capella Operational:
1. [Sign up for a Couchbase Capella account](https://docs.couchbase.com/cloud/get-started/create-account.html#sign-up-free-tier)
2. [Deploy an Operational cluster](https://docs.couchbase.com/cloud/get-started/create-account.html#getting-started)
3. [Create a bucket](https://docs.couchbase.com/cloud/clusters/data-service/manage-buckets.html#add-bucket)
    - This can be named anything, but must correspond to the bucket described in the `feature_store.yaml` configuration file.
4. [Create cluster access credentials](https://docs.couchbase.com/cloud/clusters/manage-database-users.html#create-database-credentials)
    - These credentials should have full access to the bucket created in step 3.
5. [Configure allowed IP addresses](https://docs.couchbase.com/cloud/clusters/allow-ip-address.html)
    - You must allow the IP address of the machine running Feast.

## Example
{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
  type: couchbase
  connection_string: couchbase://127.0.0.1 # Couchbase connection string, copied from 'Connect' page in Couchbase Capella console
  user: Administrator  # Couchbase username from access credentials
  password: password  # Couchbase password from access credentials
  bucket_name: feast  # Couchbase bucket name, defaults to feast
  kv_port: 11210  # Couchbase key-value port, defaults to 11210. Required if custom ports are used. 
entity_key_serialization_version: 2
```
{% endcode %}

The full set of configuration options is available in `CouchbaseOnlineStoreConfig`.


## Functionality Matrix
The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Couchbase online store.

|                                                           | Couchbase |
| :-------------------------------------------------------- | :-------- |
| write feature values to the online store                  | yes       |
| read feature values from the online store                 | yes       |
| update infrastructure (e.g. tables) in the online store   | yes       |
| teardown infrastructure (e.g. tables) in the online store | yes       |
| generate a plan of infrastructure changes                 | no        |
| support for on-demand transforms                          | yes       |
| readable by Python SDK                                    | yes       |
| readable by Java                                          | no        |
| readable by Go                                            | no        |
| support for entityless feature views                      | yes       |
| support for concurrent writing to the same key            | yes       |
| support for ttl (time to live) at retrieval               | no        |
| support for deleting expired data                         | no        |
| collocated by feature view                                | yes       |
| collocated by feature service                             | no        |
| collocated by entity key                                  | no        |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).

