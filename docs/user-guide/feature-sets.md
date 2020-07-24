# Feature Sets

Feature sets are both a schema and a means of identifying data sources for features.

Data typically comes in the form of flat files, dataframes, tables in a database, or events on a stream. Thus the data occurs with multiple columns/fields in multiple rows/events.

Feature sets are a way for defining the unique properties of these data sources, how Feast should interpret them, and how Feast should source them. Feature sets allow for groups of fields in these data sources to be [ingested](data-ingestion.md) and [stored](stores.md) together. Feature sets allow for efficient storage and logical namespacing of data within [stores](stores.md).

{% hint style="info" %}
Feature sets are a grouping of feature sets based on how they are loaded into Feast. They ensure that data is efficiently stored during ingestion. Feature sets are not a grouping of features for retrieval of features. During retrieval it is possible to retrieve feature values from any number of feature sets.
{% endhint %}

## Customer Transactions Example

Below is an example specification of a basic `customer transactions` feature set that has been exported to YAML:

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

The dataframe below \(`customer_data.csv`\) contains the features and entities of the above feature set.

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

# Apply new feature set
client.apply(cust_trans_fs)

# Load feature data into Feast for this specific feature set
client.ingest(cust_trans_fs, customer_data)
```

{% hint style="info" %}
When applying a Feature Set without specifying a project in its specification, Feast creates/updates the Feature Set in the `default` project. To create a Feature Set in another project, specify the project of choice in the Feature Set specification's project field.
{% endhint %}

## **Making changes to Feature Sets**

In order to facilitate the need for feature set definitions to change over time, a limited set of changes can be made to existing feature sets.

To apply changes to a feature set:

```python
# With existing feature set
cust_trans_fs = FeatureSet.from_yaml("customer_transactions_feature_set.yaml")

# Add new feature, avg_basket_size
cust_trans_fs.add(Feature(name="avg_basket_size", dtype=ValueType.INT32))

# Apply changed feature set
client.apply(cust_trans_fs)
```

Permitted changes include:

* Adding new features
* Deleting existing features \(note that features are tombstoned and remain on record, rather than removed completely; as a result, new features will not be able to take the names of these deleted features\)
* Changing features' TFX schemas
* Changing the feature set's source and max age

Note that the following are **not** allowed:

* Changes to project or name of the feature set.
* Changes to entities.
* Changes to names and types of existing features.

