# Feast 0.20 adds API and connector improvements

*April 21, 2022* | *Danny Chiao*

We are delighted to announce the release of Feast 0.20, which introduces many new features and enhancements:

* Many connector improvements and bug fixes (DynamoDB, Snowflake, Spark, Trino)
  * Note: Trino has been officially bundled into Feast. You can now run this with `pip install "feast[trino]"`!
* Feast API changes
* [Experimental] Feast UI as an importable npm module
* [Experimental] Python SDK with embedded Go mode

### Connector optimizations & bug fixes

Key changes:

* DynamoDB online store implementation is now much more efficient with batch feature retrieval (thanks [@TremaMiguel](https://github.com/TremaMiguel)!). As per updates on the [benchmark blog post](https://feastsite.wpenginepowered.com/blog/feast-benchmarks/), DynamoDB now is much more performant at high batch sizes for online feature retrieval!
* Snowflake offline store connector supports key pair authentication.
* Contrib plugins (documentation still pending, but see [old docs](https://github.com/Shopify/feast-trino))

### Feast API simplification

In planning for upcoming functionality (data quality monitoring, batch + stream transformations), certain parts of the Feast API are changing. As part of this change, Feast 0.20 addresses API inconsistencies. No existing feature repos will be broken, and we intend to provide a migration script to help upgrade to the latest syntax.

Key changes:

* Naming changes (e.g. `FeatureView` changes from features -> schema)
* All Feast objects will be defined with keyword args (in practice not impacting users unless they use positional args)
* Key Feast object metadata will be consistently exposed through constructors (e.g. owner, description, name)
* [Experimental] Pushing transformed features (e.g. from a stream) directly to the online store:
  * Favoring push sources

### [Experimental] Feast Web UI

See [https://github.com/feast-dev/feast/tree/master/ui](https://github.com/feast-dev/feast/tree/master/ui) to check out the new Feast Web UI! You can generate registry dumps via the Feast CLI and stand up the server at a local endpoint. You can also embed the UI as a React component and add custom tabs.

### What's next

In response to survey results (fill out this [form](https://forms.gle/9SpCeJnq3MayAqHe6) to give your input), the Feast community will be diving much more deeply into data quality monitoring, batch + stream transformations, and more performant / scalable materialization.

The community is also actively involved in many efforts. Join [#feast-web-ui](https://tectonfeast.slack.com/channels/feast-web-ui) to get involved with helping on the Feast Web UI.
