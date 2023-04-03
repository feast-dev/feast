# Rockset (contrib)

## Description

In Alpha Development.

The [Rockset](https://rockset.com/demo-signup/) online store provides support for materializing feature values within a Rockset collection for serving online features in real-time.

* Each document is uniquely identified by its '_id' value. Repeated inserts into the same document '_id' will result in an upsert.

Rockset indexes all columns allowing for quick per feature look up and also allows for a dynamic typed schema that can change based on any new requirements. ApiKeys can be found in the console
along with host urls which you can find in "View Region Endpoint Urls".

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
online_stores
    type: rockset
    apikey: MY_APIKEY_HERE
    host: api.usw2a1.rockset.com
```
