# Structuring Feature Repos

A common scenario when using Feast in production is to want to test changes to Feast object definitions. For this, we recommend setting up a _staging_ environment for your offline and online stores, which mirrors _production_ (with potentially a smaller data set). 
Having this separate environment allows users to test changes by first applying them to staging, and then promoting the changes to production after verifying the changes on staging.  

## Setting up multiple environments

There are three common ways teams approach having separate environments

1. Have separate git branches for each environment
2. Have separate `feature_store.yaml`  files and separate Feast object definitions that correspond to each environment
3. Have separate `feature_store.yaml`  files per environment, but share the Feast object definitions

### Different version control branches

To keep a clear separation of the feature repos, teams may choose to have multiple long-lived branches in their version control system, one for each environment. In this approach, with CI/CD setup, changes would first be made to the staging branch, and then copied over manually to the production branch once verified in the staging environment. 

### Separate `feature_store.yaml`  files and separate Feast object definitions

For this approach, we have created an example repository ([Feast Repository Example](https://github.com/feast-dev/feast-ci-repo-example)) which contains two Feast projects, one per environment.

The contents of this repository are shown below:

```
├── .github
│   └── workflows
│       ├── production.yml
│       └── staging.yml
│
├── staging
│   ├── driver_repo.py
│   └── feature_store.yaml
│
└── production
    ├── driver_repo.py
    └── feature_store.yaml
```

The repository contains three sub-folders:

* `staging/`: This folder contains the staging `feature_store.yaml` and Feast objects. Users that want to make changes to the Feast deployment in the staging environment will commit changes to this directory.
* `production/`: This folder contains the production `feature_store.yaml` and Feast objects. Typically users would first test changes in staging before copying the feature definitions into the production folder, before committing the changes.
* `.github`: This folder is an example of a CI system that applies the changes in either the `staging` or `production` repositories using `feast apply`. This operation saves your feature definitions to a shared registry (for example, on GCS) and configures your infrastructure for serving features.

The `feature_store.yaml` contains the following:

```
project: staging
registry: gs://feast-ci-demo-registry/staging/registry.db
provider: gcp
```

Notice how the registry has been configured to use a Google Cloud Storage bucket. All changes made to infrastructure using `feast apply` are tracked in the `registry.db`. This registry will be accessed later by the Feast SDK in your training pipelines or model serving services in order to read features.

{% hint style="success" %}
It is important to note that the CI system above must have access to create, modify, or remove infrastructure in your production environment. This is unlike clients of the feature store, who will only have read access.
{% endhint %}

If your organization consists of many independent data science teams or a single group is working on several projects that could benefit from sharing features, entities, sources, and transformations, then we encourage you to utilize Python packages inside each environment:

```
└── production
    ├── common
    │    ├── __init__.py
    │    ├── sources.py
    │    └── entities.py
    ├── ranking
    │    ├── __init__.py
    │    ├── views.py
    │    └── transformations.py
    ├── segmentation
    │    ├── __init__.py
    │    ├── views.py
    │    └── transformations.py
    └── feature_store.yaml
```


### Shared Feast Object definitions with separate `feature_store.yaml` files

This approach is very similar to the previous approach, but instead of having feast objects duplicated and having to copy over changes, it may be possible to share the same Feast object definitions and have different `feature_store.yaml` configuration. 

An example of how such a repository would be structured is as follows:

```
├── .github
│   └── workflows
│       ├── production.yml
│       └── staging.yml
├── staging
│   └── feature_store.yaml
├── production
│   └── feature_store.yaml
└── driver_repo.py
```

Users can then apply the applying them to each environment in this way:
```shell
feast -f staging/feature_store.yaml apply
```

This setup has the advantage that you can share the feature definitions entirely, which may prevent issues with copy-pasting code.

## Summary
In summary, once you have set up a Git based repository with CI that runs `feast apply` on changes, your infrastructure (offline store, online store, and cloud environment) will automatically be updated to support the loading of data into the feature store or retrieval of data.
