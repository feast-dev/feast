# Install Feast

Install Feast using [pip](https://pip.pypa.io):

```
pip install feast
```

Install Feast with Snowflake dependencies (required when using Snowflake):

```
pip install 'feast[snowflake]'
```

Install Feast with GCP dependencies (required when using BigQuery or Firestore):

```
pip install 'feast[gcp]'
```

Install Feast with AWS dependencies (required when using Redshift or DynamoDB):

```
pip install 'feast[aws]'
```

Install Feast with Redis dependencies (required when using Redis, either through AWS Elasticache or independently):

```
pip install 'feast[redis]'
```
