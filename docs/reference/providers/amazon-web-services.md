# Amazon Web Services

### Description

* Offline Store: Uses the **Redshift** offline store by default. Also supports File as the offline store.
* Online Store: Uses the **DynamoDB** online store by default. Also supports Sqlite as an online store.

### Example

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: aws
online_store:
  type: dynamodb
  region: us-west-2
offline_store:
  type: redshift
  region: us-west-2
  cluster_id: feast-cluster
  database: feast-database
  user: redshift-user
  s3_staging_location: s3://feast-bucket/redshift
  iam_role: arn:aws:iam::123456789012:role/redshift_s3_access_role
```
{% endcode %}

<!--
TODO: figure out the permissions

### **Permissions**

<table>
  <thead>
    <tr>
      <th style="text-align:left"><b>Command</b>
      </th>
      <th style="text-align:left">Component</th>
      <th style="text-align:left">Permissions</th>
      <th style="text-align:left">Recommended Role</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><b>Apply</b>
      </td>
      <td style="text-align:left">BigQuery (source)</td>
      <td style="text-align:left">
        <p>bigquery.jobs.create</p>
        <p>bigquery.readsessions.create</p>
        <p>bigquery.readsessions.getData</p>
      </td>
      <td style="text-align:left">roles/bigquery.user</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Apply</b>
      </td>
      <td style="text-align:left">Datastore (destination)</td>
      <td style="text-align:left">
        <p>datastore.entities.allocateIds</p>
        <p>datastore.entities.create</p>
        <p>datastore.entities.delete</p>
        <p>datastore.entities.get</p>
        <p>datastore.entities.list</p>
        <p>datastore.entities.update</p>
      </td>
      <td style="text-align:left">roles/datastore.owner</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Materialize</b>
      </td>
      <td style="text-align:left">BigQuery (source)</td>
      <td style="text-align:left">bigquery.jobs.create</td>
      <td style="text-align:left">roles/bigquery.user</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Materialize</b>
      </td>
      <td style="text-align:left">Datastore (destination)</td>
      <td style="text-align:left">
        <p>datastore.entities.allocateIds</p>
        <p>datastore.entities.create</p>
        <p>datastore.entities.delete</p>
        <p>datastore.entities.get</p>
        <p>datastore.entities.list</p>
        <p>datastore.entities.update</p>
        <p>datastore.databases.get</p>
      </td>
      <td style="text-align:left">roles/datastore.owner</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Get Online Features</b>
      </td>
      <td style="text-align:left">Datastore</td>
      <td style="text-align:left">datastore.entities.get</td>
      <td style="text-align:left">roles/datastore.user</td>
    </tr>
    <tr>
      <td style="text-align:left"><b>Get Historical Features</b>
      </td>
      <td style="text-align:left">BigQuery (source)</td>
      <td style="text-align:left">
        <p>bigquery.datasets.get</p>
        <p>bigquery.tables.get</p>
        <p>bigquery.tables.create</p>
        <p>bigquery.tables.updateData</p>
        <p>bigquery.tables.update</p>
        <p>bigquery.tables.delete</p>
        <p>bigquery.tables.getData</p>
      </td>
      <td style="text-align:left">roles/bigquery.dataEditor</td>
    </tr>
  </tbody>
</table>

-->
