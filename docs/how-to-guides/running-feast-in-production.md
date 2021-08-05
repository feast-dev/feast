# Running Feast in production

### Overview

In this guide we will show you how to:

1. Deploy your feature store and keep your infrastructure in sync with your feature repository
2. Keep the data in your online store up to date
3. Use Feast for model training and serving

### 1. Automatically deploying changes to your feature definitions

The first step to setting up a deployment of Feast is to create a Git repository that contains your feature definitions. The recommended way to version and track your feature definitions is by committing them to a repository and tracking changes through commits.

Most teams will need to have a feature store deployed to more than one environment. We have created an example repository \([Feast Repository Example](https://github.com/feast-dev/feast-ci-repo-example)\) which contains two Feast projects, one per environment.

The contents of this repository are shown below:

```bash
├── .github
│   └── workflows
│       ├── production.yml
│       └── staging.yml
│
├── staging
│   ├── driver_repo.py
│   └── feature_store.yaml
│
└── production
    ├── driver_repo.py
    └── feature_store.yaml

```

The repository contains three sub-folders:

* `staging/`: This folder contains the staging `feature_store.yaml` and Feast objects. Users that want to make changes to the Feast deployment in the staging environment will commit changes to this directory.
* `production/`: This folder contains the production `feature_store.yaml` and Feast objects. Typically users would first test changes in staging before copying the feature definitions into the production folder, before committing the changes.
* `.github`: This folder is an example of a CI system that applies the changes in either the `staging` or `production` repositories using `feast apply`. This operation saves your feature definitions to a shared registry \(for example, on GCS\) and configures your infrastructure for serving features.

The `feature_store.yaml` contains the following?

```text
project: staging
registry: gs://feast-ci-demo-registry/staging/registry.db
provider: gcp
```

Notice how the registry has been configured to use a Google Cloud Storage bucket. All changes made to infrastructure using `feast apply` are tracked in the `registry.db`. This registry will be accessed later by the Feast SDK in your training pipelines or model serving services in order to read features.

{% hint style="success" %}
It is important to note that the CI system above must have access to create, modify, or remove infrastructure in your production environment. This is unlike clients of the feature store, who will only have read access.
{% endhint %}

In summary, once you have set up a Git based repository with CI that runs `feast apply` on changes, your infrastructure \(offline store, online store, and cloud environment\) will automatically be updated to support loading of data into the feature store or retrieval of data.

### 2. How to keep the data in your online store up to date

In order to keep your online store up to date, you need to run a job that loads feature data from your feature view sources into your online store. In Feast, this loading operation is called materialization.

The simplest way to schedule materialization is to run an **incremental** materialization using the Feast CLI:

```text
feast materialize-incremental 2022-01-01T00:00:00
```

The above command will load all feature values from all feature view sources into the online store up to the time `2022-01-01T00:00:00`.

A timestamp is required to set the end date for materialization. If your source is fully up to date then the end date would be the current time. However, if you are querying a source where data is not yet available, then you do not want to set the timestamp to the current time. You would want to use a timestamp that ends at a date for which data is available. The next time `materialize-incremental` is run, Feast will load data that starts from the previous end date, so it is important to ensure that the materialization interval does not overlap with time periods for which data has not been made available. This is commonly the case when your source is an ETL pipeline that is scheduled on a daily basis.

An alternative approach to incremental materialization \(where Feast tracks the intervals of data that need to be ingested\), is to call Feast directly from your scheduler like Airflow. In this case Airflow is the system that tracks the intervals that have been ingested.

```text
feast materialize -v driver_hourly_stats 2020-01-01T00:00:00 2020-01-02T00:00:00
```

In the above example we are materializing the source data from the `driver_hourly_stats` feature view over a day. This command can be scheduled as the final operation in your Airflow ETL, which runs after you have computed your features and stored them in the source location. Feast will then load your feature data into your online store.

The timestamps above should match the interval of data that has been computed by the data transformation system.

### 3. How to use Feast for model training and serving

Now that you have deployed a registry, provisioned your feature store, and loaded your data into your online store, your clients can start to consume features for training and inference.

For both model training and inferencing your clients will use the Feast Python SDK to retrieve features. In both cases it is necessary to create a `FeatureStore` object.

One way to ensure your production clients have access to the feature store is to provide a copy of the `feature_store.yaml` to those pipelines. This `feature_store.yaml` file will have a reference to the feature store registry, which allows clients to retrieve features from offline or online stores.

```python
prod_fs = FeatureStore(repo_path="production_feature_store.yaml")
```

Then, training data can be retrieved as follows:

```python
feature_refs = [
    'driver_hourly_stats:conv_rate',
    'driver_hourly_stats:acc_rate',
    'driver_hourly_stats:avg_daily_trips'
]

training_df = prod_fs.get_historical_features(
    entity_df=entity_df, 
    feature_refs=feature_refs,
).to_df()

model = ml.fit(training_df)
```

The most common way to productionize ML models is by storing and versioning models in a "model store", and then deploying these models into production. When using Feast, it is recommended that the list of feature references also be saved alongside the model. This ensures that models and the features they are trained on are paired together when being shipped into production:

```python
# Save model
model.save('my_model.bin')

# Save features
open('feature_refs.json', 'w') as f:
    json.dump(feature_refs, f)
```

you can simply create a `FeatureStore` object, fetch the features, and then make a prediction:

```python
# Load model
model = ml.load('my_model.bin')

# Load feature references
with open('feature_refs.json', 'r') as f:
    feature_refs = json.load(f)

# Create feature store object
prod_fs = FeatureStore(repo_path="production_feature_store.yaml")

# Read online features
feature_vector = prod_fs.get_online_features(
    feature_refs=feature_refs,
    entity_rows=[{"driver_id": 1001}]
).to_dict()

# Make a prediction
prediction = model.predict(feature_vector)
```

{% hint style="success" %}
It is important to note that both the training pipeline and model serving service only needs read access to the feature registry and associated infrastructure. This prevents clients from accidentally making changes to the feature store.
{% endhint %}

