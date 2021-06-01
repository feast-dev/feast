# Quickstart

In this guide we will
* Set up a local Feast deployment with a Parquet file offline store and Sqlite online store.
* Build a training dataset using time series features from the bundled Parquet files.
* Read the latest features from the Sqlite online store for inference.

## Install Feast

Install the Feast SDK and CLI using pip:

```bash
pip install feast
```

## Create a feature repository

Bootstrap a new feature repository using `feast init`:

{{ get_code_block('init') }}

## Register feature definitions and deploy your feature store

The `apply` command registers all the objects in your feature repository and deploys a feature store:

{{ get_code_block('apply') }}

## Generating training data

The `apply` command builds a training dataset based on the time-series features defined in the feature repository:

{{ get_code_block('training') }}

## Load features into your online store

The `materialize` command loads the latest feature values from your feature views into your online store:

{{ get_code_block('materialize') }}

## Fetching feature vectors for inference

{{ get_code_block('predict') }}

## Next steps

Now that you've tried out a minimal deployment of Feast, have a look at the following resources
* Follow our [Getting Started guide](getting-started/) for a hands tutorial in using Feast
* Join our [Slack group](https://slack.com) to talk to ask questions, speak to other users, and become part of the community!