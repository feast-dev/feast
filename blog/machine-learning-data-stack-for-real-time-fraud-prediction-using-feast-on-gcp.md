# Machine learning data stack for real-time fraud detection using Feast on GCP

*September 8, 2021* | *Jay Parthasarthy and Jules S. Damji*

A machine learning (ML) model decides whether your transaction is blocked or approved every time you purchase using your credit card. Fraud detection is a canonical use case for real-time ML. Predictions are made upon each request quickly while you wait at a point of sale for payment approval.

Even though this is a common problem with ML, companies often build custom tooling to tackle these predictions. Like most ML problems, the hard part of fraud prediction is in the data. The fundamental data challenges are the following:

1. Some data needed for prediction is available as part of the transaction request. This data is the easy part of passing to the model.
2. Other data (for example, a user's historical purchases) provides a high signal for predictions, but it isn't available as part of the transaction request. This data takes time to look up: it's stored in a batch system like a data warehouse. This data is challenging to fetch since it requires a system to handle many queries per second (QPS).
3. Together, they comprise ML features as signals to the model for predicting whether the requested transaction is fraudulent.

[Feast](https://feastsite.wpenginepowered.com/) is an open-source feature store that helps teams use batch data for real-time ML applications. It's used as part of fraud [prediction and other high-volume transactions systems](https://www.youtube.com/watch?v=ED81DvicQuQ) to prevent fraud for billions of dollars worth of transactions at companies like [Gojek](https://www.gojek.com/en-id/) and [Postmates](https://postmates.com/). In this blog, we discuss how we can use Feast to build a stack for fraud predictions. You can also follow along on Google Cloud Platform (GCP) by running this [Colab tutorial notebook](https://colab.research.google.com/github/feast-dev/feast-fraud-tutorial).

## Generic data stack for fraud detection

Here's what a generic stack for fraud prediction looks like:

## 1. Generating batch features from data sources

The first step in deploying an ML model is to generate features from raw data stored in an offline system, such as a data warehouse (DWH) or a modern data lake. After that, we use these features in our ML model for training and inference. But before we get into the specifics of fraud detection related to our example below, let's quickly understand some high-level concepts.

Data sources: This data repository records all historical transactions data for a user, account information, and any indication of user fraud history. Usually, it's a data warehouse (DHW) with respective tables. The diagram above shows that features are generated from these data sources and put into another offline store (or the same store). Using transformational queries, like SQL, this data, joined from multiple tables, could be injected or stored as another table in a DWH— refined and computed as features.

Features used: In the fraud use case, one set of the raw data is a record of historical transactions. This record includes data about the transaction:
* Amount of transaction
* Timestamp when the event occurred
* User account information

## 3. Materialize features to low-latency online stores

We have a model that's ready for real-time inference. However, we won't be able to make predictions in real-time if we need to fetch or compute data out of the data warehouse on each request because it's slow.

Feast allows you to make real-time predictions based on warehouse data by materializing it into an [online store](https://docs.feastsite.wpenginepowered.com/concepts/registry). Using the Feast CLI, you can incrementally materialize your data, from the current time on since the previous materialized data:

```bash
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

With our feature values loaded into the online store, a low-latency key-value store, as shown in the diagram above, we can retrieve new data when a new transaction request arrives in our system.

Note that the feast materialize-incremental command needs to be run regularly so that the online store can continue to contain fresh feature values. We suggest that you integrate this command into your company's scheduler (e.g., Airflow.)

## Conclusion

In summation, we outlined a general data stack for real-time fraudulent prediction use cases. We implemented an end-to-end fraud prediction system using [Feast on GCP](https://github.com/feast-dev/feast-fraud-tutorial) as part of our tutorial.

We'd love to hear how your organization's setup differs. This setup roughly corresponds to the most common patterns we've seen from our users, but things are usually more complicated as teams introduce feature logging, streaming features, and operational databases.

You can bootstrap a simple stack illustrated in this blog by running our [tutorial notebook on GCP](https://colab.research.google.com/github/feast-dev/feast-fraud-tutorial). From there, you can integrate your prediction service into your production application and start making predictions in real-time. We can't wait to see what you build with Feast, and please share with the [Feast community](http://slack.feastsite.wpenginepowered.com/).
