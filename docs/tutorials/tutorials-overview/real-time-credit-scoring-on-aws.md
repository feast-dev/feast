---
description: >-
  Credit scoring models are used to approve or reject loan applications. In this
  tutorial we will build a real-time credit scoring system on AWS.
---

# Real-time credit scoring on AWS

When individuals apply for loans from banks and other credit providers, the decision to approve a loan application is often made through a statistical model. This model uses information about a customer to determine the likelihood that they will repay or default on a loan, in a process called credit scoring.

In this example, we will demonstrate how a real-time credit scoring system can be built using Feast and Scikit-Learn on AWS, using feature data from S3.

This real-time system accepts a loan request from a customer and responds within 100ms with a decision on whether their loan has been approved or rejected.

## [Real-time Credit Scoring Example](https://github.com/feast-dev/real-time-credit-scoring-on-aws-tutorial)

This end-to-end tutorial will take you through the following steps:

* Deploying S3 with Parquet as your primary data source, containing both [loan features](https://github.com/feast-dev/real-time-credit-scoring-on-aws-tutorial/blob/22fc6c7272ef033e7ba0afc64ffaa6f6f8fc0277/data/loan\_table\_sample.csv) and [zip code features](https://github.com/feast-dev/real-time-credit-scoring-on-aws-tutorial/blob/22fc6c7272ef033e7ba0afc64ffaa6f6f8fc0277/data/zipcode\_table\_sample.csv)
* Deploying Redshift as the interface Feast uses to build training datasets
* Registering your features with Feast and configuring DynamoDB for online serving
* Building a training dataset with Feast to train your credit scoring model
* Loading feature values from S3 into DynamoDB
* Making online predictions with your credit scoring model using features from DynamoDB

| ![](../../.gitbook/assets/github-mark-32px.png)[ View Source on Github](https://github.com/feast-dev/real-time-credit-scoring-on-aws-tutorial) |
| ---------------------------------------------------------------------------------------------------------------------------------------------- |
