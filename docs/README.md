# Overview

Feast \(**Fea**ture **St**ore\) is a tool for managing and serving machine learning features.

> Feast is the bridge between your models and your data

Feast aims to:

* Provide a unified means of managing feature data from a single person to large enterprises.
* Provide scalable and performant access to feature data when training and serving models.
* Provide consistent and point-in-time correct access to feature data.
* Enable discovery, documentation, and insights into your features.

![](.gitbook/assets/feast-docs-overview-diagram-2%20%283%29.svg)

**TL;DR:** Feast decouples feature engineering from feature usage. Features that are added to Feast become available immediately for training and serving. Models can retrieve the same features used in training from a low latency online store in production.

This means that new ML projects start with a process of feature selection from a catalog instead of having to do feature engineering from scratch.

```python
# Setting things up
fs = feast.Client('feast.example.com')
customer_ids = ['1001', '1002', '1003']
customer_features = ['CreditScore', 'Balance', 'Age', 'NumOfProducts', 'IsActive']
from_date = '2019-01-01'
to_date = '2019-12-31'

# Training your model (typically from a notebook or pipeline)
data = fs.get_batch_features(customer_features, customer_ids, from_date, to_date)
my_model = ml.fit(data.to_train(), data.to_train())

# Serving predictions (when serving the model in production)
prediction = my_model.predict(fs.get_online_features(customer_features, customer_ids))
```

The code above is for illustrative purposes. Please see our getting started guide for more realistic examples.

For more reasons to use Feast, please see [Why Feast?](why-feast.md#why-feast)

