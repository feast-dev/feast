import React, { useState, useEffect } from "react";
import {
  EuiFormRow,
  EuiFieldText,
  EuiSelect,
  EuiSpacer,
  EuiHorizontalRule,
  EuiText,
  EuiCallOut,
  EuiFlexGroup,
  EuiFlexItem,
  EuiPanel,
  EuiTextArea,
  EuiTitle,
} from "@elastic/eui";
import { feast } from "../protos";
import FormModal from "./forms/FormModal";
import TagsEditor, { TagEntry } from "./forms/TagsEditor";
import { DATA_SOURCE_TYPES } from "../pages/data-sources/DataSourceCatalog";

const SOURCE_TYPE_OPTIONS = [
  {
    value: String(feast.core.DataSource.SourceType.BATCH_FILE),
    text: "File (Parquet / CSV)",
  },
  {
    value: String(feast.core.DataSource.SourceType.BATCH_BIGQUERY),
    text: "BigQuery",
  },
  {
    value: String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE),
    text: "Snowflake",
  },
  {
    value: String(feast.core.DataSource.SourceType.BATCH_REDSHIFT),
    text: "Redshift",
  },
  {
    value: String(feast.core.DataSource.SourceType.BATCH_SPARK),
    text: "Spark",
  },
  {
    value: String(feast.core.DataSource.SourceType.BATCH_TRINO),
    text: "Trino",
  },
  {
    value: String(feast.core.DataSource.SourceType.BATCH_ATHENA),
    text: "AWS Athena",
  },
  {
    value: String(feast.core.DataSource.SourceType.STREAM_KAFKA),
    text: "Kafka",
  },
  {
    value: String(feast.core.DataSource.SourceType.STREAM_KINESIS),
    text: "AWS Kinesis",
  },
  {
    value: String(feast.core.DataSource.SourceType.REQUEST_SOURCE),
    text: "Request Source",
  },
  {
    value: String(feast.core.DataSource.SourceType.PUSH_SOURCE),
    text: "Push Source",
  },
  {
    value: String(feast.core.DataSource.SourceType.CUSTOM_SOURCE),
    text: "Custom Source",
  },
  { value: "RAY_SOURCE", text: "Ray" },
  { value: "POSTGRES_SOURCE", text: "PostgreSQL" },
  { value: "MONGODB_SOURCE", text: "MongoDB" },
  { value: "CLICKHOUSE_SOURCE", text: "ClickHouse" },
  { value: "MSSQL_SOURCE", text: "SQL Server" },
  { value: "ORACLE_SOURCE", text: "Oracle" },
  { value: "COUCHBASE_SOURCE", text: "Couchbase" },
];

interface DataSourceFormData {
  name: string;
  description: string;
  owner: string;
  sourceType: string;
  timestampField: string;
  createdTimestampColumn: string;
  tags: TagEntry[];
  fileUri: string;
  bigqueryTable: string;
  bigqueryQuery: string;
  snowflakeTable: string;
  snowflakeDatabase: string;
  snowflakeSchema: string;
  redshiftTable: string;
  redshiftDatabase: string;
  redshiftSchema: string;
  kafkaBootstrapServers: string;
  kafkaTopic: string;
  sparkTable: string;
  sparkPath: string;
  kinesisRegion: string;
  kinesisStreamName: string;
  trinoTable: string;
  trinoQuery: string;
  athenaTable: string;
  athenaQuery: string;
  athenaDatabase: string;
  athenaDataSource: string;
  customSourceClassName: string;
  customSourceConfig: string;
  // Contrib source fields
  rayReaderType: string;
  rayPath: string;
  rayReaderOptions: string;
  postgresTable: string;
  postgresQuery: string;
  mongodbCollection: string;
  clickhouseTable: string;
  clickhouseQuery: string;
  mssqlTable: string;
  mssqlConnectionStr: string;
  oracleTable: string;
  oracleConnectionStr: string;
  couchbaseDatabase: string;
  couchbaseScope: string;
  couchbaseCollection: string;
  couchbaseQuery: string;
}

interface DataSourceFormModalProps {
  onClose: () => void;
  onSubmit: (data: DataSourceFormData) => void;
  initialData?: DataSourceFormData;
  isEdit?: boolean;
  isSubmitting?: boolean;
  submitError?: string | null;
}

const EMPTY_FORM: DataSourceFormData = {
  name: "",
  description: "",
  owner: "",
  sourceType: String(feast.core.DataSource.SourceType.BATCH_FILE),
  timestampField: "",
  createdTimestampColumn: "",
  tags: [],
  fileUri: "",
  bigqueryTable: "",
  bigqueryQuery: "",
  snowflakeTable: "",
  snowflakeDatabase: "",
  snowflakeSchema: "",
  redshiftTable: "",
  redshiftDatabase: "",
  redshiftSchema: "",
  kafkaBootstrapServers: "",
  kafkaTopic: "",
  sparkTable: "",
  sparkPath: "",
  kinesisRegion: "",
  kinesisStreamName: "",
  trinoTable: "",
  trinoQuery: "",
  athenaTable: "",
  athenaQuery: "",
  athenaDatabase: "",
  athenaDataSource: "",
  customSourceClassName: "",
  customSourceConfig: "",
  rayReaderType: "parquet",
  rayPath: "",
  rayReaderOptions: "",
  postgresTable: "",
  postgresQuery: "",
  mongodbCollection: "",
  clickhouseTable: "",
  clickhouseQuery: "",
  mssqlTable: "",
  mssqlConnectionStr: "",
  oracleTable: "",
  oracleConnectionStr: "",
  couchbaseDatabase: "",
  couchbaseScope: "",
  couchbaseCollection: "",
  couchbaseQuery: "",
};

const BATCH_SOURCE_TYPES = new Set([
  String(feast.core.DataSource.SourceType.BATCH_FILE),
  String(feast.core.DataSource.SourceType.BATCH_BIGQUERY),
  String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE),
  String(feast.core.DataSource.SourceType.BATCH_REDSHIFT),
  String(feast.core.DataSource.SourceType.BATCH_SPARK),
  String(feast.core.DataSource.SourceType.BATCH_TRINO),
  String(feast.core.DataSource.SourceType.BATCH_ATHENA),
  "RAY_SOURCE",
  "POSTGRES_SOURCE",
  "MONGODB_SOURCE",
  "CLICKHOUSE_SOURCE",
  "MSSQL_SOURCE",
  "ORACLE_SOURCE",
  "COUCHBASE_SOURCE",
]);

const RAY_READER_OPTIONS = [
  { value: "parquet", text: "Parquet" },
  { value: "csv", text: "CSV" },
  { value: "json", text: "JSON" },
  { value: "text", text: "Text" },
  { value: "images", text: "Images" },
  { value: "binary_files", text: "Binary Files" },
  { value: "tfrecords", text: "TFRecords" },
  { value: "webdataset", text: "WebDataset" },
  { value: "huggingface", text: "HuggingFace" },
  { value: "mongo", text: "MongoDB (via Ray)" },
  { value: "sql", text: "SQL (via Ray)" },
];

const DataSourceFormModal: React.FC<DataSourceFormModalProps> = ({
  onClose,
  onSubmit,
  initialData,
  isEdit = false,
  isSubmitting = false,
  submitError,
}) => {
  const [formData, setFormData] = useState<DataSourceFormData>(
    initialData || EMPTY_FORM,
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState(false);

  const isBatchSource = BATCH_SOURCE_TYPES.has(formData.sourceType);
  const isPreselected = !!initialData?.sourceType;

  const catalogEntry = DATA_SOURCE_TYPES.find(
    (ds) => ds.sourceType === formData.sourceType,
  );

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};
    const st = formData.sourceType;

    if (!formData.name.trim()) {
      newErrors.name = "Data source name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formData.name)) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, and underscores.";
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_FILE)) {
      if (!formData.fileUri.trim()) {
        newErrors.fileUri = "File URI is required.";
      } else if (
        !/^(s3|gs|gcs|hdfs|abfs|file):\/\/\S+$/.test(formData.fileUri.trim())
      ) {
        newErrors.fileUri =
          "Must be a valid URI (e.g. s3://bucket/path, gs://bucket/path, file:///local/path).";
      }
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_BIGQUERY)) {
      if (!formData.bigqueryTable.trim() && !formData.bigqueryQuery.trim()) {
        newErrors.bigqueryTable =
          "Either a table reference or a query is required.";
      }
    } else if (
      st === String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE)
    ) {
      if (!formData.snowflakeTable.trim()) {
        newErrors.snowflakeTable = "Table name is required for Snowflake.";
      }
      if (!formData.snowflakeDatabase.trim()) {
        newErrors.snowflakeDatabase = "Database is required for Snowflake.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_REDSHIFT)) {
      if (!formData.redshiftTable.trim()) {
        newErrors.redshiftTable = "Table name is required for Redshift.";
      }
      if (!formData.redshiftDatabase.trim()) {
        newErrors.redshiftDatabase = "Database is required for Redshift.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_SPARK)) {
      if (!formData.sparkTable.trim() && !formData.sparkPath.trim()) {
        newErrors.sparkTable =
          "Either a table reference or a path is required for Spark.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_TRINO)) {
      if (!formData.trinoTable.trim() && !formData.trinoQuery.trim()) {
        newErrors.trinoTable =
          "Either a table reference or a query is required for Trino.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_ATHENA)) {
      if (!formData.athenaTable.trim() && !formData.athenaQuery.trim()) {
        newErrors.athenaTable =
          "Either a table reference or a query is required for Athena.";
      }
      if (!formData.athenaDatabase.trim()) {
        newErrors.athenaDatabase = "Database is required for Athena.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.STREAM_KAFKA)) {
      if (!formData.kafkaBootstrapServers.trim()) {
        newErrors.kafkaBootstrapServers = "Bootstrap servers are required.";
      } else if (
        !/^[\w.-]+:\d+(,[\w.-]+:\d+)*$/.test(
          formData.kafkaBootstrapServers.trim(),
        )
      ) {
        newErrors.kafkaBootstrapServers =
          "Must be in host:port format (e.g. localhost:9092).";
      }
      if (!formData.kafkaTopic.trim()) {
        newErrors.kafkaTopic = "Topic is required.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.STREAM_KINESIS)) {
      if (!formData.kinesisRegion.trim()) {
        newErrors.kinesisRegion = "AWS region is required.";
      }
      if (!formData.kinesisStreamName.trim()) {
        newErrors.kinesisStreamName = "Stream name is required.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.CUSTOM_SOURCE)) {
      if (!formData.customSourceClassName.trim()) {
        newErrors.customSourceClassName = "Class name is required.";
      }
    } else if (st === "RAY_SOURCE") {
      if (
        !formData.rayPath.trim() &&
        !["huggingface", "mongo", "sql"].includes(formData.rayReaderType)
      ) {
        newErrors.rayPath = "Path is required for this reader type.";
      }
    } else if (st === "POSTGRES_SOURCE") {
      if (!formData.postgresTable.trim() && !formData.postgresQuery.trim()) {
        newErrors.postgresTable = "Either a table or query is required.";
      }
    } else if (st === "CLICKHOUSE_SOURCE") {
      if (
        !formData.clickhouseTable.trim() &&
        !formData.clickhouseQuery.trim()
      ) {
        newErrors.clickhouseTable = "Either a table or query is required.";
      }
    } else if (st === "MSSQL_SOURCE") {
      if (!formData.mssqlTable.trim()) {
        newErrors.mssqlTable = "Table reference is required.";
      }
    } else if (st === "ORACLE_SOURCE") {
      if (!formData.oracleTable.trim()) {
        newErrors.oracleTable = "Table reference is required.";
      }
    } else if (st === "COUCHBASE_SOURCE") {
      if (
        !formData.couchbaseCollection.trim() &&
        !formData.couchbaseQuery.trim()
      ) {
        newErrors.couchbaseCollection =
          "Either a collection or query is required.";
      }
    }

    if (isBatchSource && !formData.timestampField.trim()) {
      newErrors.timestampField =
        "Timestamp field is required for batch sources.";
    } else if (
      formData.timestampField.trim() &&
      !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formData.timestampField.trim())
    ) {
      newErrors.timestampField =
        "Must be a valid column name (letters, numbers, underscores).";
    }

    const tagKeys = formData.tags.map((t) => t.key).filter((k) => k.trim());
    if (new Set(tagKeys).size !== tagKeys.length) {
      newErrors.tags = "Tag keys must be unique.";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = () => {
    setSubmitted(true);
    if (validate()) {
      const cleanedData = {
        ...formData,
        tags: formData.tags.filter((t) => t.key.trim()),
      };
      onSubmit(cleanedData);
    }
  };

  const updateField = <K extends keyof DataSourceFormData>(
    field: K,
    value: DataSourceFormData[K],
  ) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
    if (submitted) {
      setErrors((prev) => {
        const next = { ...prev };
        delete next[field];
        return next;
      });
    }
  };

  const renderFileSourceFields = () => (
    <EuiFormRow
      label="File Path / URI"
      isInvalid={!!errors.fileUri}
      error={errors.fileUri}
      helpText="Path to the data file accessible by the Feast server (e.g. s3://bucket/path/data.parquet, gs://bucket/data.csv, file:///mnt/data/features.parquet)."
    >
      <EuiFieldText
        value={formData.fileUri}
        onChange={(e) => updateField("fileUri", e.target.value)}
        isInvalid={!!errors.fileUri}
        placeholder="s3://bucket/path/to/data.parquet"
      />
    </EuiFormRow>
  );

  const renderSourceTypeHeader = () => {
    if (!isPreselected || !catalogEntry) return null;

    const IconComponent = catalogEntry.icon;
    return (
      <EuiPanel
        color="subdued"
        paddingSize="m"
        hasBorder={false}
        style={{ borderLeft: `4px solid ${catalogEntry.color}` }}
      >
        <EuiFlexGroup alignItems="center" gutterSize="m" responsive={false}>
          <EuiFlexItem grow={false}>
            <div
              style={{
                width: 36,
                height: 36,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
              }}
            >
              <IconComponent width={28} height={28} />
            </div>
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiText size="s">
              <strong>{catalogEntry.name}</strong>
            </EuiText>
            <EuiText size="xs" color="subdued">
              {catalogEntry.description}
            </EuiText>
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>
    );
  };

  const renderSourceTypeFields = () => {
    const st = formData.sourceType;

    if (st === String(feast.core.DataSource.SourceType.BATCH_FILE)) {
      return renderFileSourceFields();
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_BIGQUERY)) {
      return (
        <>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.bigqueryTable}
            error={errors.bigqueryTable}
            helpText="Full table reference: project:dataset.table"
          >
            <EuiFieldText
              value={formData.bigqueryTable}
              onChange={(e) => updateField("bigqueryTable", e.target.value)}
              isInvalid={!!errors.bigqueryTable}
              placeholder="project:dataset.table"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Query (optional)"
            helpText="SQL query as an alternative to a fixed table."
          >
            <EuiTextArea
              value={formData.bigqueryQuery}
              onChange={(e) => updateField("bigqueryQuery", e.target.value)}
              placeholder="SELECT * FROM `project.dataset.table` WHERE ..."
              rows={3}
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE)) {
      return (
        <>
          <EuiFlexGroup gutterSize="m">
            <EuiFlexItem>
              <EuiFormRow
                label="Database"
                isInvalid={!!errors.snowflakeDatabase}
                error={errors.snowflakeDatabase}
              >
                <EuiFieldText
                  value={formData.snowflakeDatabase}
                  onChange={(e) =>
                    updateField("snowflakeDatabase", e.target.value)
                  }
                  isInvalid={!!errors.snowflakeDatabase}
                  placeholder="MY_DATABASE"
                />
              </EuiFormRow>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiFormRow label="Schema">
                <EuiFieldText
                  value={formData.snowflakeSchema}
                  onChange={(e) =>
                    updateField("snowflakeSchema", e.target.value)
                  }
                  placeholder="PUBLIC"
                />
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.snowflakeTable}
            error={errors.snowflakeTable}
          >
            <EuiFieldText
              value={formData.snowflakeTable}
              onChange={(e) => updateField("snowflakeTable", e.target.value)}
              isInvalid={!!errors.snowflakeTable}
              placeholder="MY_TABLE"
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_REDSHIFT)) {
      return (
        <>
          <EuiFlexGroup gutterSize="m">
            <EuiFlexItem>
              <EuiFormRow
                label="Database"
                isInvalid={!!errors.redshiftDatabase}
                error={errors.redshiftDatabase}
              >
                <EuiFieldText
                  value={formData.redshiftDatabase}
                  onChange={(e) =>
                    updateField("redshiftDatabase", e.target.value)
                  }
                  isInvalid={!!errors.redshiftDatabase}
                  placeholder="my_database"
                />
              </EuiFormRow>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiFormRow label="Schema">
                <EuiFieldText
                  value={formData.redshiftSchema}
                  onChange={(e) =>
                    updateField("redshiftSchema", e.target.value)
                  }
                  placeholder="public"
                />
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.redshiftTable}
            error={errors.redshiftTable}
          >
            <EuiFieldText
              value={formData.redshiftTable}
              onChange={(e) => updateField("redshiftTable", e.target.value)}
              isInvalid={!!errors.redshiftTable}
              placeholder="my_table"
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.STREAM_KAFKA)) {
      return (
        <>
          <EuiFormRow
            label="Bootstrap Servers"
            isInvalid={!!errors.kafkaBootstrapServers}
            error={errors.kafkaBootstrapServers}
            helpText="Comma-separated host:port pairs."
          >
            <EuiFieldText
              value={formData.kafkaBootstrapServers}
              onChange={(e) =>
                updateField("kafkaBootstrapServers", e.target.value)
              }
              isInvalid={!!errors.kafkaBootstrapServers}
              placeholder="broker1:9092,broker2:9092"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Topic"
            isInvalid={!!errors.kafkaTopic}
            error={errors.kafkaTopic}
          >
            <EuiFieldText
              value={formData.kafkaTopic}
              onChange={(e) => updateField("kafkaTopic", e.target.value)}
              isInvalid={!!errors.kafkaTopic}
              placeholder="my-feature-topic"
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_SPARK)) {
      return (
        <>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.sparkTable}
            error={errors.sparkTable}
            helpText="Spark catalog table (catalog.database.table). Provide either table or path."
          >
            <EuiFieldText
              value={formData.sparkTable}
              onChange={(e) => updateField("sparkTable", e.target.value)}
              isInvalid={!!errors.sparkTable}
              placeholder="catalog.database.table"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Path (optional)"
            helpText="Alternative: direct path to data files."
          >
            <EuiFieldText
              value={formData.sparkPath}
              onChange={(e) => updateField("sparkPath", e.target.value)}
              placeholder="s3://bucket/path/"
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_TRINO)) {
      return (
        <>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.trinoTable}
            error={errors.trinoTable}
            helpText="Trino catalog table (catalog.schema.table). Provide either table or query."
          >
            <EuiFieldText
              value={formData.trinoTable}
              onChange={(e) => updateField("trinoTable", e.target.value)}
              isInvalid={!!errors.trinoTable}
              placeholder="catalog.schema.table"
            />
          </EuiFormRow>
          <EuiFormRow label="Query (optional)">
            <EuiTextArea
              value={formData.trinoQuery}
              onChange={(e) => updateField("trinoQuery", e.target.value)}
              placeholder="SELECT * FROM catalog.schema.table"
              rows={3}
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_ATHENA)) {
      return (
        <>
          <EuiFlexGroup gutterSize="m">
            <EuiFlexItem>
              <EuiFormRow
                label="Database"
                isInvalid={!!errors.athenaDatabase}
                error={errors.athenaDatabase}
              >
                <EuiFieldText
                  value={formData.athenaDatabase}
                  onChange={(e) =>
                    updateField("athenaDatabase", e.target.value)
                  }
                  isInvalid={!!errors.athenaDatabase}
                  placeholder="my_database"
                />
              </EuiFormRow>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiFormRow label="Data Source" helpText="Athena catalog name.">
                <EuiFieldText
                  value={formData.athenaDataSource}
                  onChange={(e) =>
                    updateField("athenaDataSource", e.target.value)
                  }
                  placeholder="AwsDataCatalog"
                />
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.athenaTable}
            error={errors.athenaTable}
            helpText="Provide either a table name or a query."
          >
            <EuiFieldText
              value={formData.athenaTable}
              onChange={(e) => updateField("athenaTable", e.target.value)}
              isInvalid={!!errors.athenaTable}
              placeholder="my_table"
            />
          </EuiFormRow>
          <EuiFormRow label="Query (optional)">
            <EuiTextArea
              value={formData.athenaQuery}
              onChange={(e) => updateField("athenaQuery", e.target.value)}
              placeholder="SELECT * FROM my_table"
              rows={3}
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.STREAM_KINESIS)) {
      return (
        <>
          <EuiFormRow
            label="AWS Region"
            isInvalid={!!errors.kinesisRegion}
            error={errors.kinesisRegion}
          >
            <EuiFieldText
              value={formData.kinesisRegion}
              onChange={(e) => updateField("kinesisRegion", e.target.value)}
              isInvalid={!!errors.kinesisRegion}
              placeholder="us-east-1"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Stream Name"
            isInvalid={!!errors.kinesisStreamName}
            error={errors.kinesisStreamName}
          >
            <EuiFieldText
              value={formData.kinesisStreamName}
              onChange={(e) => updateField("kinesisStreamName", e.target.value)}
              isInvalid={!!errors.kinesisStreamName}
              placeholder="my-feature-stream"
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.CUSTOM_SOURCE)) {
      return (
        <>
          <EuiFormRow
            label="Data Source Class"
            isInvalid={!!errors.customSourceClassName}
            error={errors.customSourceClassName}
            helpText="Fully qualified Python class name."
          >
            <EuiFieldText
              value={formData.customSourceClassName}
              onChange={(e) =>
                updateField("customSourceClassName", e.target.value)
              }
              isInvalid={!!errors.customSourceClassName}
              placeholder="mymodule.MyCustomDataSource"
            />
          </EuiFormRow>
          <EuiFormRow label="Configuration (JSON, optional)">
            <EuiTextArea
              value={formData.customSourceConfig}
              onChange={(e) =>
                updateField("customSourceConfig", e.target.value)
              }
              placeholder='{"key": "value"}'
              rows={3}
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === "RAY_SOURCE") {
      return (
        <>
          <EuiFormRow
            label="Reader Type"
            helpText="Ray Data reader to use for loading data."
          >
            <EuiSelect
              options={RAY_READER_OPTIONS}
              value={formData.rayReaderType}
              onChange={(e) => updateField("rayReaderType", e.target.value)}
            />
          </EuiFormRow>
          <EuiFormRow
            label="Path"
            isInvalid={!!errors.rayPath}
            error={errors.rayPath}
            helpText="File path or directory for file-based readers (s3://, gs://, local)."
          >
            <EuiFieldText
              value={formData.rayPath}
              onChange={(e) => updateField("rayPath", e.target.value)}
              isInvalid={!!errors.rayPath}
              placeholder="s3://bucket/images/"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Reader Options (JSON, optional)"
            helpText='e.g. {"dataset_name": "org/dataset", "split": "train"} for HuggingFace'
          >
            <EuiTextArea
              value={formData.rayReaderOptions}
              onChange={(e) => updateField("rayReaderOptions", e.target.value)}
              placeholder='{"dataset_name": "org/name", "split": "train"}'
              rows={3}
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === "POSTGRES_SOURCE") {
      return (
        <>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.postgresTable}
            error={errors.postgresTable}
            helpText="Table name. Provide either table or query."
          >
            <EuiFieldText
              value={formData.postgresTable}
              onChange={(e) => updateField("postgresTable", e.target.value)}
              isInvalid={!!errors.postgresTable}
              placeholder="public.my_features"
            />
          </EuiFormRow>
          <EuiFormRow label="Query (optional)">
            <EuiTextArea
              value={formData.postgresQuery}
              onChange={(e) => updateField("postgresQuery", e.target.value)}
              placeholder="SELECT * FROM my_features WHERE ..."
              rows={3}
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === "MONGODB_SOURCE") {
      return (
        <EuiFormRow
          label="Collection"
          isInvalid={!!errors.mongodbCollection}
          error={errors.mongodbCollection}
          helpText="MongoDB collection name. Connection details are configured in feature_store.yaml."
        >
          <EuiFieldText
            value={formData.mongodbCollection}
            onChange={(e) => updateField("mongodbCollection", e.target.value)}
            isInvalid={!!errors.mongodbCollection}
            placeholder="features_collection"
          />
        </EuiFormRow>
      );
    }

    if (st === "CLICKHOUSE_SOURCE") {
      return (
        <>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.clickhouseTable}
            error={errors.clickhouseTable}
            helpText="ClickHouse table name. Provide either table or query."
          >
            <EuiFieldText
              value={formData.clickhouseTable}
              onChange={(e) => updateField("clickhouseTable", e.target.value)}
              isInvalid={!!errors.clickhouseTable}
              placeholder="default.my_features"
            />
          </EuiFormRow>
          <EuiFormRow label="Query (optional)">
            <EuiTextArea
              value={formData.clickhouseQuery}
              onChange={(e) => updateField("clickhouseQuery", e.target.value)}
              placeholder="SELECT * FROM default.my_features"
              rows={3}
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === "MSSQL_SOURCE") {
      return (
        <>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.mssqlTable}
            error={errors.mssqlTable}
          >
            <EuiFieldText
              value={formData.mssqlTable}
              onChange={(e) => updateField("mssqlTable", e.target.value)}
              isInvalid={!!errors.mssqlTable}
              placeholder="dbo.my_features"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Connection String (optional)"
            helpText="ODBC-style connection string. Can also be set in feature_store.yaml."
          >
            <EuiFieldText
              value={formData.mssqlConnectionStr}
              onChange={(e) =>
                updateField("mssqlConnectionStr", e.target.value)
              }
              placeholder="mssql+pyodbc://user:pass@host/db" // pragma: allowlist secret
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === "ORACLE_SOURCE") {
      return (
        <>
          <EuiFormRow
            label="Table"
            isInvalid={!!errors.oracleTable}
            error={errors.oracleTable}
          >
            <EuiFieldText
              value={formData.oracleTable}
              onChange={(e) => updateField("oracleTable", e.target.value)}
              isInvalid={!!errors.oracleTable}
              placeholder="SCHEMA.MY_FEATURES"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Connection String (optional)"
            helpText="Oracle connection string. Can also be set in feature_store.yaml."
          >
            <EuiFieldText
              value={formData.oracleConnectionStr}
              onChange={(e) =>
                updateField("oracleConnectionStr", e.target.value)
              }
              placeholder="oracle+cx_oracle://user:pass@host:1521/service" // pragma: allowlist secret
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === "COUCHBASE_SOURCE") {
      return (
        <>
          <EuiFlexGroup gutterSize="m">
            <EuiFlexItem>
              <EuiFormRow label="Database">
                <EuiFieldText
                  value={formData.couchbaseDatabase}
                  onChange={(e) =>
                    updateField("couchbaseDatabase", e.target.value)
                  }
                  placeholder="Default"
                />
              </EuiFormRow>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiFormRow label="Scope">
                <EuiFieldText
                  value={formData.couchbaseScope}
                  onChange={(e) =>
                    updateField("couchbaseScope", e.target.value)
                  }
                  placeholder="Default"
                />
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiFormRow
            label="Collection"
            isInvalid={!!errors.couchbaseCollection}
            error={errors.couchbaseCollection}
            helpText="Provide either a collection or a SQL++ query."
          >
            <EuiFieldText
              value={formData.couchbaseCollection}
              onChange={(e) =>
                updateField("couchbaseCollection", e.target.value)
              }
              isInvalid={!!errors.couchbaseCollection}
              placeholder="my_collection"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Query (optional)"
            helpText="SQL++ query as an alternative."
          >
            <EuiTextArea
              value={formData.couchbaseQuery}
              onChange={(e) => updateField("couchbaseQuery", e.target.value)}
              placeholder="SELECT * FROM `collection`"
              rows={3}
            />
          </EuiFormRow>
        </>
      );
    }

    if (
      st === String(feast.core.DataSource.SourceType.REQUEST_SOURCE) ||
      st === String(feast.core.DataSource.SourceType.PUSH_SOURCE)
    ) {
      return (
        <EuiPanel color="subdued" paddingSize="m">
          <EuiText size="s" color="subdued">
            No connection configuration needed. This source type receives data
            at request time or via push ingestion.
          </EuiText>
        </EuiPanel>
      );
    }

    return null;
  };

  const sourceTypeName =
    SOURCE_TYPE_OPTIONS.find((o) => o.value === formData.sourceType)?.text ||
    "Data Source";

  return (
    <FormModal
      title={
        isEdit
          ? `Edit ${sourceTypeName}`
          : isPreselected
            ? `New ${sourceTypeName} Connection`
            : "Create Data Source"
      }
      submitLabel={isEdit ? "Update" : "Create Connection"}
      onClose={onClose}
      onSubmit={handleSubmit}
      width={720}
      isSubmitting={isSubmitting}
    >
      {submitError && (
        <>
          <EuiCallOut
            title="Unable to save data source"
            color="danger"
            iconType="alert"
            size="s"
          >
            <p>{submitError}</p>
          </EuiCallOut>
          <EuiSpacer size="m" />
        </>
      )}

      {isPreselected && renderSourceTypeHeader()}
      {isPreselected && <EuiSpacer size="m" />}

      {/* Section: Identity */}
      <EuiTitle size="xxs">
        <h4>Identity</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiFlexGroup gutterSize="m">
        <EuiFlexItem>
          <EuiFormRow
            label="Name"
            isInvalid={!!errors.name}
            error={errors.name}
            helpText="Unique identifier for this data source."
          >
            <EuiFieldText
              value={formData.name}
              onChange={(e) => updateField("name", e.target.value)}
              isInvalid={!!errors.name}
              disabled={isEdit}
              placeholder="e.g. customer_transactions"
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiFormRow label="Owner (optional)">
            <EuiFieldText
              value={formData.owner}
              onChange={(e) => updateField("owner", e.target.value)}
              placeholder="team@company.com"
            />
          </EuiFormRow>
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiFormRow label="Description (optional)">
        <EuiFieldText
          value={formData.description}
          onChange={(e) => updateField("description", e.target.value)}
          placeholder="Brief description of this data source..."
        />
      </EuiFormRow>

      {!isPreselected && (
        <>
          <EuiSpacer size="m" />
          <EuiFormRow label="Source Type">
            <EuiSelect
              options={SOURCE_TYPE_OPTIONS}
              value={formData.sourceType}
              onChange={(e) => {
                updateField("sourceType", e.target.value);
                setErrors({});
              }}
              disabled={isEdit}
            />
          </EuiFormRow>
        </>
      )}

      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />

      {/* Section: Connection */}
      <EuiTitle size="xxs">
        <h4>Connection Details</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      {renderSourceTypeFields()}

      {/* Section: Timestamp (for batch sources) */}
      {isBatchSource && (
        <>
          <EuiSpacer size="m" />
          <EuiHorizontalRule margin="s" />
          <EuiTitle size="xxs">
            <h4>Time Configuration</h4>
          </EuiTitle>
          <EuiSpacer size="s" />

          <EuiFlexGroup gutterSize="m">
            <EuiFlexItem>
              <EuiFormRow
                label="Timestamp Field"
                isInvalid={!!errors.timestampField}
                error={errors.timestampField}
                helpText="Event timestamp column for point-in-time joins."
              >
                <EuiFieldText
                  value={formData.timestampField}
                  onChange={(e) =>
                    updateField("timestampField", e.target.value)
                  }
                  isInvalid={!!errors.timestampField}
                  placeholder="event_timestamp"
                />
              </EuiFormRow>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiFormRow
                label="Created Timestamp (optional)"
                helpText="Used for deduplication."
              >
                <EuiFieldText
                  value={formData.createdTimestampColumn}
                  onChange={(e) =>
                    updateField("createdTimestampColumn", e.target.value)
                  }
                  placeholder="created_at"
                />
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>
        </>
      )}

      {/* Section: Tags */}
      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />
      <EuiTitle size="xxs">
        <h4>Tags (optional)</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <TagsEditor
        tags={formData.tags}
        onChange={(tags) => updateField("tags", tags)}
        error={errors.tags}
      />
    </FormModal>
  );
};

export default DataSourceFormModal;
export type { DataSourceFormData };
