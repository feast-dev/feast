# Feast Quickstart
A quick view of what's in this repository:

* `data/` contains raw demo parquet data
* `example_repo.py` contains demo feature definitions
* `feature_store.yaml` contains a demo setup configuring where data sources are
* `test_workflow.py` showcases how to run all key Feast commands, including defining, retrieving, and pushing features. 

You can run the overall workflow with `python test_workflow.py`.

## To move from this into a more production ready workflow:
1. `feature_store.yaml` points to a local file as a registry. You'll want to setup a remote file (e.g. in S3/GCS) or a 
SQL registry. See [registry docs](https://docs.feast.dev/getting-started/concepts/registry) for more details. 
2. This example uses an already setup BigQuery Feast data warehouse as the [offline store](https://docs.feast.dev/getting-started/components/offline-store) 
   to generate training data. You'll need to connect your own BigQuery instance to make this work.
3. Setup CI/CD + dev vs staging vs prod environments to automatically update the registry as you change Feast feature definitions. See [docs](https://docs.feast.dev/how-to-guides/running-feast-in-production#1.-automatically-deploying-changes-to-your-feature-definitions).
4. (optional) Regularly scheduled materialization to power low latency feature retrieval (e.g. via Airflow). See [Batch data ingestion](https://docs.feast.dev/getting-started/concepts/data-ingestion#batch-data-ingestion)
for more details. 
5. (optional) Deploy feature server instances with `feast serve` to expose endpoints to retrieve online features.
   - See [Python feature server](https://docs.feast.dev/reference/feature-servers/python-feature-server) for details.
   - Use cases can also directly call the Feast client to fetch features as per [Feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval)