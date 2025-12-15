# Feast Quickstart
If you haven't already, check out the quickstart guide on Feast's website (http://docs.feast.dev/quickstart), which 
uses this repo. A quick view of what's in this repository's `feature_repo/` directory:

* `data/` contains raw demo parquet data
* `feature_repo/feature_definitions.py` contains demo feature definitions
* `feature_repo/feature_store.yaml` contains a demo setup configuring where data sources are
* `feature_repo/test_workflow.py` showcases how to run all key Feast commands, including defining, retrieving, and pushing features. 

You can run the overall workflow with `python test_workflow.py`.

## To move from this into a more production ready workflow:
> See more details in [Running Feast in production](https://docs.feast.dev/how-to-guides/running-feast-in-production)

1. First: you should start with a different Feast template, which delegates to a more scalable offline store. 
   - For example, running `feast init -t gcp`
   or `feast init -t aws` or `feast init -t snowflake`. 
   - You can see your options if you run `feast init --help`.
2. `feature_store.yaml` points to a local file as a registry. You'll want to setup a remote file (e.g. in S3/GCS) or a 
SQL registry. See [registry docs](https://docs.feast.dev/getting-started/concepts/registry) for more details. 
3. This example uses a file [offline store](https://docs.feast.dev/getting-started/components/offline-store) 
   to generate training data. It does not scale. We recommend instead using a data warehouse such as BigQuery, 
   Snowflake, Redshift. There is experimental support for Spark as well.
4. Setup CI/CD + dev vs staging vs prod environments to automatically update the registry as you change Feast feature definitions. See [docs](https://docs.feast.dev/how-to-guides/running-feast-in-production#1.-automatically-deploying-changes-to-your-feature-definitions).
5. (optional) Regularly scheduled materialization to power low latency feature retrieval (e.g. via Airflow). See [Batch data ingestion](https://docs.feast.dev/getting-started/concepts/data-ingestion#batch-data-ingestion)
for more details.
6. (optional) Deploy feature server instances with `feast serve` to expose endpoints to retrieve online features.
   - See [Python feature server](https://docs.feast.dev/reference/feature-servers/python-feature-server) for details.
   - Use cases can also directly call the Feast client to fetch features as per [Feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval)