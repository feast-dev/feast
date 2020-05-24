# Feature Sets

Feature sets are both a schema and a means of identifying data sources for features.

Data typically comes in the form of flat files, dataframes, tables in a database, or events on a stream. Thus the data occurs with multiple columns/fields in multiple rows/events. 

Feature sets are a way for defining the unique properties of these data sources, how Feast should interpret them, and how Feast should source them. Feature sets allow for groups of fields in these data sources to be [ingested](data-ingestion.md) and [stored](stores.md) together. Feature sets allow for efficient storage and logical namespacing of data within [stores](stores.md).

{% hint style="info" %}
Feature sets are a grouping of feature sets based on how they are loaded into Feast. They ensure that data is efficiently stored during ingestion. Feature sets are not a grouping of features for retrieval of features. During retrieval it is possible to retrieve feature values from any number of feature sets.
{% endhint %}

### Customer Transactions Example

Below is an example of a basic `customer transactions` feature set that has been exported to YAML:

{% tabs %}
{% tab title="customer\_transactions\_feature\_set.yaml" %}
```yaml
name: customer_transactions
entities:
- name: customer_id
  valueType: INT64
features:
- name: daily_transactions
  valueType: FLOAT
- name: total_transactions
  valueType: FLOAT
```
{% endtab %}
{% endtabs %}

The dataframe below \(`customer_data.csv`\) contains the features and entities of the above feature set

| datetime | customer\_id | daily\_transactions | total\_tra**nsactions** |
| :--- | :--- | :--- | :--- |
| 2019-01-01 01:00:00 | 20001 | 5.0 | 14.0 |
| 2019-01-01 01:00:00 | 20002 | 2.6 | 43.0 |
| 2019-01-01 01:00:00 | 20003 | 4.1 | 154.0 |
| 2019-01-01 01:00:00 | 20004 | 3.4 | 74.0 |

In order to ingest feature data into Feast for this specific feature set:

```python
# Load dataframe
customer_df = pd.read_csv("customer_data.csv")

# Create feature set from YAML (using YAML is optional)
cust_trans_fs = FeatureSet.from_yaml("customer_transactions_feature_set.yaml")

# Load feature data into Feast for this specific feature set
client.ingest(cust_trans_fs, customer_data)
```

