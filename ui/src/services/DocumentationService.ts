const DocumentationService = {
  async fetchCLIDocumentation(): Promise<string> {
    try {
      return `# Feast CLI Reference

## feast apply

Apply changes to a feature store. This command should be run after modifying
feature definitions.

**Usage:**

\`\`\`
feast apply
\`\`\`

**Options:**

- \`--skip-source-validation\`: Skip validation of data sources.

---

## feast materialize

Materialize features from an offline store into an online store.

**Usage:**

\`\`\`
feast materialize START_TS END_TS
\`\`\`

**Options:**

- \`--views\`: Feature views to materialize.

---

## feast registry-dump

Dump registry contents to local file.

**Usage:**

\`\`\`
feast registry-dump REGISTRY_PATH
\`\`\`

---

## feast serve

Start a feature server.

**Usage:**

\`\`\`
feast serve
\`\`\`

**Options:**

- \`--host\`: Specify host for the server.
- \`--port\`: Specify port for the server.
`;
    } catch (error) {
      console.error("Error fetching CLI documentation:", error);
      return "# Error\nFailed to load CLI documentation.";
    }
  },

  async fetchSDKDocumentation(): Promise<string> {
    try {
      return `# Feast SDK Reference

## FeatureStore

The main entry point for interacting with Feast.

\`\`\`python
from feast import FeatureStore

fs = FeatureStore(repo_path="path/to/feature_repo")
\`\`\`

### Methods

- \`apply()\`: Register feature definitions to the feature store.
- \`get_historical_features()\`: Retrieve historical feature values.
- \`get_online_features()\`: Retrieve the latest feature values.
- \`materialize()\`: Materialize features from offline to online store.

## FeatureView

Define a group of features that share the same data source and entities.

\`\`\`python
from feast import FeatureView, Feature, Entity, ValueType

driver = Entity(name="driver_id", value_type=ValueType.INT64)

driver_stats_view = FeatureView(
    name="driver_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
    ],
    batch_source=FileSource(path="path/to/data.parquet"),
)
\`\`\`

## Entity

Define an entity to join feature values with.

\`\`\`python
from feast import Entity, ValueType

driver = Entity(
    name="driver_id",
    value_type=ValueType.INT64,
    description="Driver ID",
)
\`\`\`
`;
    } catch (error) {
      console.error("Error fetching SDK documentation:", error);
      return "# Error\nFailed to load SDK documentation.";
    }
  },

  async fetchAPIDocumentation(): Promise<string> {
    try {
      return `# Feast REST API Reference

## Feature Server Endpoints

### GET /health

Health check endpoint.

**Response:**

\`\`\`json
{
  "status": "ok"
}
\`\`\`

### POST /get-online-features

Retrieve the latest feature values.

**Request:**

\`\`\`json
{
  "features": [
    "driver_stats:conv_rate",
    "driver_stats:acc_rate"
  ],
  "entities": {
    "driver_id": [1001, 1002]
  }
}
\`\`\`

**Response:**

\`\`\`json
{
  "metadata": {
    "feature_names": ["driver_stats:conv_rate", "driver_stats:acc_rate"]
  },
  "results": [
    {
      "values": [0.95, 0.79],
      "statuses": ["PRESENT", "PRESENT"]
    },
    {
      "values": [0.83, 0.85],
      "statuses": ["PRESENT", "PRESENT"]
    }
  ]
}
\`\`\`

### POST /push

Push feature values to the online store.

**Request:**

\`\`\`json
{
  "push_source_name": "driver_stats_push_source",
  "df": {
    "driver_id": [1001, 1002],
    "conv_rate": [0.95, 0.83],
    "acc_rate": [0.79, 0.85],
    "event_timestamp": ["2022-01-01T00:00:00Z", "2022-01-01T00:00:00Z"]
  }
}
\`\`\`

**Response:**

\`\`\`json
{
  "status": "success"
}
\`\`\`
`;
    } catch (error) {
      console.error("Error fetching API documentation:", error);
      return "# Error\nFailed to load API documentation.";
    }
  },
};

export default DocumentationService;
