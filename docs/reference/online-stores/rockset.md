# Rockset (contrib)

## Description

In Alpha Development.

The [Rockset](https://rockset.com/demo-signup/) online store provides support for materializing feature values within a Rockset collection in order to serve features in real-time.

* Each document is uniquely identified by its '_id' value. Repeated inserts into the same document '_id' will result in an upsert.

Rockset indexes all columns allowing for quick per feature look up and also allows for a dynamic typed schema that can change based on any new requirements. API Keys can be found in the Rockset console.
You can also find host urls on the same tab by clicking "View Region Endpoint Urls".

Data Model Used Per Doc

```
{
  "_id": (STRING) Unique Identifier for the feature document.
  <key_name>: (STRING) Feature Values Mapped by Feature Name. Feature
                       values stored as a serialized hex string.
  ....
  "event_ts": (STRING) ISO Stringified Timestamp.
  "created_ts": (STRING) ISO Stringified Timestamp.
}
```


## Example

```yaml
project: my_feature_app
registry: data/registry.db
provider: local
online_store:
    ## Basic Configs ##

    # If apikey or host is left blank the driver will try to pull
    # these values from environment variables ROCKSET_APIKEY and 
    # ROCKSET_APISERVER respectively.
    type: rockset
    api_key: <your_api_key_here>
    host: <your_region_endpoint_here>
  
    ## Advanced Configs ## 

    # Batch size of records that will be turned per page when
    # paginating a batched read.
    #
    # read_pagination_batch_size: 100

    # The amount of time, in seconds, we will wait for the
    # collection to become visible to the API.
    #
    # collection_created_timeout_secs: 60

    # The amount of time, in seconds, we will wait for the
    # collection to enter READY state.
    #
    # collection_ready_timeout_secs: 1800

    # Whether to wait for all writes to be flushed from log
    # and queryable before returning write as completed. If
    # False, documents that are written may not be seen
    # immediately in subsequent reads.
    #
    # fence_all_writes: True

    # The amount of time we will wait, in seconds, for the
    # write fence to be passed
    #
    # fence_timeout_secs: 600

    # Initial backoff, in seconds, we will wait between
    # requests when polling for a response.
    #
    # initial_request_backoff_secs: 2

    # Initial backoff, in seconds, we will wait between
    # requests when polling for a response.
    # max_request_backoff_secs: 30

    # The max amount of times we will retry a failed request.
    # max_request_attempts: 10000
```
