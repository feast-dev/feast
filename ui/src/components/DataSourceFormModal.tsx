import React, { useState, useEffect } from "react";
import {
  EuiFormRow,
  EuiFieldText,
  EuiSelect,
  EuiSpacer,
  EuiHorizontalRule,
  EuiText,
  EuiCallOut,
} from "@elastic/eui";
import { feast } from "../protos";
import FormModal from "./forms/FormModal";
import TagsEditor, { TagEntry } from "./forms/TagsEditor";
import NameDescriptionOwnerFields from "./forms/NameDescriptionOwnerFields";

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
    value: String(feast.core.DataSource.SourceType.STREAM_KAFKA),
    text: "Kafka",
  },
  {
    value: String(feast.core.DataSource.SourceType.BATCH_SPARK),
    text: "Spark",
  },
  {
    value: String(feast.core.DataSource.SourceType.REQUEST_SOURCE),
    text: "Request Source",
  },
  {
    value: String(feast.core.DataSource.SourceType.PUSH_SOURCE),
    text: "Push Source",
  },
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
};

const BATCH_SOURCE_TYPES = new Set([
  String(feast.core.DataSource.SourceType.BATCH_FILE),
  String(feast.core.DataSource.SourceType.BATCH_BIGQUERY),
  String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE),
  String(feast.core.DataSource.SourceType.BATCH_REDSHIFT),
  String(feast.core.DataSource.SourceType.BATCH_SPARK),
]);

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

  useEffect(() => {
    if (initialData) {
      setFormData(initialData);
    }
  }, [initialData]);

  const isBatchSource = BATCH_SOURCE_TYPES.has(formData.sourceType);

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};
    const st = formData.sourceType;

    if (!formData.name.trim()) {
      newErrors.name = "Data source name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formData.name)) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, and underscores.";
    }

    // Source-type-specific required fields
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
    }

    // Timestamp field required for batch sources (needed for point-in-time correctness)
    if (isBatchSource && !formData.timestampField.trim()) {
      newErrors.timestampField =
        "Timestamp field is required for batch sources. It is used for point-in-time correct feature retrieval.";
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

  const renderSourceTypeFields = () => {
    const st = formData.sourceType;

    if (st === String(feast.core.DataSource.SourceType.BATCH_FILE)) {
      return (
        <EuiFormRow
          label="File URI"
          isInvalid={!!errors.fileUri}
          error={errors.fileUri}
          helpText="Path to parquet or CSV file(s). Supports s3://, gs://, and file:// schemes."
        >
          <EuiFieldText
            value={formData.fileUri}
            onChange={(e) => updateField("fileUri", e.target.value)}
            isInvalid={!!errors.fileUri}
            placeholder="s3://bucket/path/to/data.parquet"
          />
        </EuiFormRow>
      );
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
            label="Query"
            helpText="Optional SQL query — use instead of a fixed table reference."
          >
            <EuiFieldText
              value={formData.bigqueryQuery}
              onChange={(e) => updateField("bigqueryQuery", e.target.value)}
              placeholder="SELECT * FROM `project.dataset.table`"
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE)) {
      return (
        <>
          <EuiFormRow
            label="Database"
            isInvalid={!!errors.snowflakeDatabase}
            error={errors.snowflakeDatabase}
          >
            <EuiFieldText
              value={formData.snowflakeDatabase}
              onChange={(e) => updateField("snowflakeDatabase", e.target.value)}
              isInvalid={!!errors.snowflakeDatabase}
              placeholder="MY_DATABASE"
            />
          </EuiFormRow>
          <EuiFormRow label="Schema">
            <EuiFieldText
              value={formData.snowflakeSchema}
              onChange={(e) => updateField("snowflakeSchema", e.target.value)}
              placeholder="PUBLIC"
            />
          </EuiFormRow>
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
          <EuiFormRow
            label="Database"
            isInvalid={!!errors.redshiftDatabase}
            error={errors.redshiftDatabase}
          >
            <EuiFieldText
              value={formData.redshiftDatabase}
              onChange={(e) => updateField("redshiftDatabase", e.target.value)}
              isInvalid={!!errors.redshiftDatabase}
              placeholder="my_database"
            />
          </EuiFormRow>
          <EuiFormRow label="Schema">
            <EuiFieldText
              value={formData.redshiftSchema}
              onChange={(e) => updateField("redshiftSchema", e.target.value)}
              placeholder="public"
            />
          </EuiFormRow>
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
            helpText="Comma-separated list of broker host:port pairs."
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
            helpText="Spark catalog table reference (catalog.database.table). Provide either table or path."
          >
            <EuiFieldText
              value={formData.sparkTable}
              onChange={(e) => updateField("sparkTable", e.target.value)}
              isInvalid={!!errors.sparkTable}
              placeholder="catalog.database.table"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Path"
            helpText="Alternative to table: path to data files (s3://, gs://, hdfs://)."
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

    if (
      st === String(feast.core.DataSource.SourceType.REQUEST_SOURCE) ||
      st === String(feast.core.DataSource.SourceType.PUSH_SOURCE)
    ) {
      return (
        <EuiText size="s" color="subdued">
          No additional configuration required for this source type.
        </EuiText>
      );
    }

    return null;
  };

  return (
    <FormModal
      title={isEdit ? "Edit Data Source" : "Create Data Source"}
      submitLabel={isEdit ? "Update Data Source" : "Create Data Source"}
      onClose={onClose}
      onSubmit={handleSubmit}
      width={650}
      isSubmitting={isSubmitting}
    >
      {submitError && (
        <>
          <EuiCallOut
            title={
              isEdit
                ? "Unable to update data source"
                : "Unable to create data source"
            }
            color="danger"
            iconType="alert"
            size="s"
          >
            <p>{submitError}</p>
          </EuiCallOut>
          <EuiSpacer size="m" />
        </>
      )}

      <NameDescriptionOwnerFields
        name={formData.name}
        description={formData.description}
        owner={formData.owner}
        onChangeName={(v) => updateField("name", v)}
        onChangeDescription={(v) => updateField("description", v)}
        onChangeOwner={(v) => updateField("owner", v)}
        nameDisabled={isEdit}
        nameError={errors.name}
        nameHelpText="A unique name for this data source."
        namePlaceholder="e.g. customer_transactions"
        descriptionPlaceholder="Describe this data source..."
      />

      <EuiFormRow
        label="Source Type"
        helpText="The type of underlying storage system for this data source."
      >
        <EuiSelect
          options={SOURCE_TYPE_OPTIONS}
          value={formData.sourceType}
          onChange={(e) => {
            updateField("sourceType", e.target.value);
            // Clear source-specific errors when type changes
            setErrors((prev) => {
              const next = { ...prev };
              delete next.fileUri;
              delete next.bigqueryTable;
              delete next.snowflakeTable;
              delete next.snowflakeDatabase;
              delete next.redshiftTable;
              delete next.redshiftDatabase;
              delete next.kafkaBootstrapServers;
              delete next.kafkaTopic;
              delete next.sparkTable;
              delete next.timestampField;
              return next;
            });
          }}
          disabled={isEdit}
        />
      </EuiFormRow>

      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />
      <EuiText size="s">
        <h4>Source Configuration</h4>
      </EuiText>
      <EuiSpacer size="s" />

      {renderSourceTypeFields()}

      {isBatchSource && (
        <>
          <EuiSpacer size="m" />
          <EuiFormRow
            label="Timestamp Field"
            isInvalid={!!errors.timestampField}
            error={errors.timestampField}
            helpText="Column containing the event timestamp. Required for point-in-time correct feature retrieval."
          >
            <EuiFieldText
              value={formData.timestampField}
              onChange={(e) => updateField("timestampField", e.target.value)}
              isInvalid={!!errors.timestampField}
              placeholder="event_timestamp"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Created Timestamp Column"
            helpText="Optional: column tracking when the row was written (used for deduplication)."
          >
            <EuiFieldText
              value={formData.createdTimestampColumn}
              onChange={(e) =>
                updateField("createdTimestampColumn", e.target.value)
              }
              placeholder="created_at"
            />
          </EuiFormRow>
        </>
      )}

      <EuiSpacer size="m" />

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
