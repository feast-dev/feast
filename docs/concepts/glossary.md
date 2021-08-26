# Glossary

{% hint style="danger" %}
We strongly encourage all users to upgrade from Feast 0.9 to Feast 0.10+. Please see [this](https://docs.feast.dev/v/master/project/feast-0.9-vs-feast-0.10+) for an explanation of the differences between the two versions. A guide to upgrading can be found [here](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0/edit#heading=h.9gb2523q4jlh). 
{% endhint %}

## **Entity key**

The combination of entities that uniquely identify a row. For example a feature table with the composite entity of \(customer, country\) might have an entity key of \(1001, 5\). They key is used during lookups of feature values and for deduplicating historical rows.

## Entity timestamp

The timestamp on which an event occurred. The entity timestamp could describe the event time at which features were calculated, or it could describe the event timestamps at which outcomes were observed.

Entity timestamps are commonly found on the entity dataframe and associated with the target variable \(outcome\) that needs to be predicted. These timestamps are the target on which point-in-time joins should be made.

## Entity rows

A combination of a single [entity key ](glossary.md#entity-key)and a single [entity timestamp](glossary.md#entity-timestamp).

## Entity dataframe

A collection of [entity rows](glossary.md#entity-rows). This dataframe is enriched with feature values before being used for model training.

## Feature References

Feature references uniquely identify feature values throughout Feast. Feature references can either be defined as objects or as strings.

The structure of a feature reference in string form is as follows:

`feature_table:feature`

Example:

`drivers_stream:unique_drivers`

Feature references are unique within a project. It is not possible to reference \(or retrieve\) features from multiple projects at the same time.

\*\*\*\*

