# Create a feature repository

A feature repository is a directory that contains the configuration of the feature store and individual features. This configuration is written as code \(Python/YAML\) and it's highly recommended that teams track it centrally using git. See [Feature Repository](../../reference/feature-repository/) for a detailed explanation of feature repositories.

The easiest way to create a new feature repository to use `feast init` command:

{% tabs %}
{% tab title="Local template" %}
```bash
feast init

Creating a new Feast repository in /<...>/tiny_pika.
```
{% endtab %}

{% tabs %}
{% tab title="Snowflake template" %}
```bash
feast init -t snowflake
Snowflake Deployment URL: ...
Snowflake User Name: ...
Snowflake Password: ...
Snowflake Role Name: ...
Snowflake Warehouse Name: ...
Snowflake Database Name: ...

Creating a new Feast repository in /<...>/tiny_pika.
```
{% endtab %}

{% tab title="GCP template" %}
```text
feast init -t gcp

Creating a new Feast repository in /<...>/tiny_pika.
```
{% endtab %}

{% tab title="AWS template" %}
```text
feast init -t aws
AWS Region (e.g. us-west-2): ...
Redshift Cluster ID: ...
Redshift Database Name: ...
Redshift User Name: ...
Redshift S3 Staging Location (s3://*): ...
Redshift IAM Role for S3 (arn:aws:iam::*:role/*): ...
Should I upload example data to Redshift (overwriting 'feast_driver_hourly_stats' table)? (Y/n):

Creating a new Feast repository in /<...>/tiny_pika.
```
{% endtab %}
{% endtabs %}

The `init` command creates a Python file with feature definitions, sample data, and a Feast configuration file for local development:

```bash
$ tree
.
└── tiny_pika
    ├── data
    │   └── driver_stats.parquet
    ├── example.py
    └── feature_store.yaml

1 directory, 3 files
```

Enter the directory:

```text
# Replace "tiny_pika" with your auto-generated dir name
cd tiny_pika
```

You can now use this feature repository for development. You can try the following:

* Run `feast apply` to apply these definitions to Feast.
* Edit the example feature definitions in  `example.py` and run `feast apply` again to change feature definitions.
* Initialize a git repository in the same directory and checking the feature repository into version control.
