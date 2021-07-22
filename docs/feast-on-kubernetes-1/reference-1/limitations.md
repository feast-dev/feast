# Limitations

### Feast API

<table>
  <thead>
    <tr>
      <th style="text-align:left">Limitation</th>
      <th style="text-align:left">Motivation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">Features names and entity names cannot overlap in feature table definitions</td>
      <td
      style="text-align:left">Features and entities become columns in historical stores which may cause
        conflicts</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p>The following field names are reserved in feature tables</p>
        <ul>
          <li><code>event_timestamp</code>
          </li>
          <li><code>datetime</code>
          </li>
          <li><code>created_timestamp</code>
          </li>
          <li><code>ingestion_id</code>
          </li>
          <li><code>job_id</code>
          </li>
        </ul>
      </td>
      <td style="text-align:left">These keywords are used for column names when persisting metadata in historical
        stores</td>
    </tr>
  </tbody>
</table>

### Ingestion

| Limitation | Motivation |
| :--- | :--- |
| Once data has been ingested into Feast, there is currently no way to delete the data without manually going to the database and deleting it. However, during retrieval only the latest rows will be returned for a specific key \(`event_timestamp`, `entity`\) based on its `created_timestamp`. | This functionality simply doesn't exist yet as a Feast API |

### Storage

| Limitation | Motivation |
| :--- | :--- |
| Feast does not support offline storage in Feast 0.8 | As part of our re-architecture of Feast, we moved from GCP to cloud-agnostic deployments. Developing offline storage support that is available in all cloud environments is a pending action. |

