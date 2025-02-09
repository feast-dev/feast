# Feast 0.18 adds Snowflake support and data quality monitoring

*February 14, 2022* | *Felix Wang*

We are delighted to announce the release of Feast [0.18](https://github.com/feast-dev/feast/releases/tag/v0.18.0), which introduces several new features and other improvements:

* Snowflake offline store, which allows you to define and use features stored in Snowflake.
* [Experimental] Saved Datasets, which allow training datasets to be persisted in an offline store.
* [Experimental] Data quality monitoring, which allows you to validate your training data with Great Expectations. Future work will allow you to detect issues with upstream data pipelines and check for training-serving skew.
* Python feature server graduation from alpha status.
* Performance improvements to on demand feature views, protobuf serialization and deserialization, and the Python feature server.

Experimental features are subject to API changes in the near future as we collect feedback. If you have thoughts, please don't hesitate to reach out to the Feast team through our [Slack](http://slack.feastsite.wpenginepowered.com/)!

### Snowflake offline store

Prior to Feast 0.18, Feast had first-class support for Google BigQuery and AWS Redshift as offline stores. In addition, there were various plugins for Snowflake, Azure, Postgres, and Hive. Feast 0.18 introduces first-class support for Snowflake as an offline store, so users can more easily leverage features defined in Snowflake. The Snowflake offline store can be used with the AWS, GCP, and Azure providers.

### [Experimental] Saved Datasets

Training datasets generated via `get_historical_features` can now be persisted in an offline store and reused later. This functionality will be primarily needed to generate reference datasets for validation purposes (see next section) but also could be useful in other use cases like caching results of a computationally intensive point-in-time join.

### [Experimental] Data quality monitoring

Feast 0.18 includes the first milestone of our data quality monitoring work. Many users have requested ways to validate their training and serving data, as well as monitor for training-serving skew. Feast 0.18 allows users to validate their training data through an integration with [Great Expectations](https://greatexpectations.io/). Users can declare one of the previously generated training datasets as a reference for this validation by persisting it as a "saved dataset" (see previous section). More details about future milestones of data quality monitoring can be found [here](https://docs.feastsite.wpenginepowered.com/v/master/reference/data-quality). There's also a [tutorial on validating historical features](https://docs.feastsite.wpenginepowered.com/v/master/how-to-guides/validation/validating-historical-features) that demonstrates all new concepts in action.

### Performance improvements

The Feast team and community members have made several significant performance improvements. For example, the Python feature server performance was improved by switching to a more efficient serving interface. Improving our protobuf serialization and deserialization logic led to speedups in on demand feature views. The Datastore implementation was also sped up by batching operations. For more details, please see our [blog post](https://feastsite.wpenginepowered.com/blog/feast-benchmarks/) with detailed benchmarks!

### What's next

We are collaborating with the community on the first milestone of the `feast plan` command, future milestones of data quality monitoring, and a consolidation of our online serving logic into Golang.

In addition, there is active community work on adding support for Snowflake as an online store, merging the Azure plugin into the main Feast repo, and more. If you have thoughts on what to build next in Feast, please fill out this [form](https://docs.google.com/forms/d/e/1FAIpQLSfa1nR).

Download Feast 0.18 today from [PyPI](https://pypi.org/project/feast/)
