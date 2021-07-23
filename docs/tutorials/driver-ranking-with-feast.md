---
description: >-
  Making a prediction using a linear regression model is a common use case in
  ML. This model predicts if a driver will complete a trip based on a features
  ingested into Feast.
---

# Driver ranking

In this example you'll learn how to use some of the key functionality in Feast. The tutorial runs in both local mode and on the Google Cloud Platform \(GCP\). For GCP, you must have access to a GCP project already, including read and write permissions to BigQuery.

### [Driver Ranking Example](https://github.com/feast-dev/feast-driver-ranking-tutorial)

This tutorial guides you in how to use Feast with [scikit-learn](https://scikit-learn.org/stable/). You will learn how to:

1. Train a model locally \(on your laptop\) using data from [BigQuery](https://cloud.google.com/bigquery/)
2. Test the model for online inference using [SQLite](https://www.sqlite.org/index.html) \(for fast iteration\)
3. Test the model for online inference using [Firestore](https://firebase.google.com/products/firestore) \(for production use\)

Try it and let us know what you think!

