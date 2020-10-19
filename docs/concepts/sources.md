# Sources

A `source` is a data source that can be used to find feature data. Users define sources as part of [feature tables](feature-tables.md).

Currently, Feast supports the following sources.

<table>
  <thead>
    <tr>
      <th style="text-align:left">Source type</th>
      <th style="text-align:left">Sources</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">Batch Source</td>
      <td style="text-align:left">
        <ul>
          <li>File</li>
          <li><a href="https://cloud.google.com/bigquery">BigQuery</a>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">Stream Source</td>
      <td style="text-align:left">
        <ul>
          <li><a href="https://kafka.apache.org/">Kafka</a>
          </li>
          <li><a href="https://aws.amazon.com/kinesis/">Kinesis</a>
          </li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

More information about the options to be specified for the above supported sources can be found [here](https://api.docs.feast.dev/grpc/feast.core.pb.html#DataSource).

{% hint style="info" %}
A batch source with data already materialised in it must be specified during the creation of a Feature Table for creation of training datasets.
{% endhint %}

An example of a user provided source can be seen in the following code snippet.

```python
from feast import BigQuerySource

batch_source = BigQuerySource(
    table_ref="gcp_project:bq_dataset.bq_table",
    event_timestamp_column="datetime",
    created_timestamp_column="timestamp",
)
```

Feast will ensure that the source complies with the schema of the feature table. 

This specified batch source will then be included inside a feature table specification and used for registration, which will be explained in the next section.

