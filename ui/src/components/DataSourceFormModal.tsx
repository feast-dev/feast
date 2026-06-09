import React, { useState, useEffect } from "react";
import {
  EuiFormRow,
  EuiFieldText,
  EuiSelect,
  EuiSpacer,
  EuiHorizontalRule,
  EuiText,
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

const DataSourceFormModal: React.FC<DataSourceFormModalProps> = ({
  onClose,
  onSubmit,
  initialData,
  isEdit = false,
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

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name.trim()) {
      newErrors.name = "Data source name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formData.name)) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, and underscores.";
    }

    const st = formData.sourceType;
    if (st === String(feast.core.DataSource.SourceType.BATCH_FILE)) {
      if (!formData.fileUri.trim()) {
        newErrors.fileUri = "File URI is required for file sources.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_BIGQUERY)) {
      if (!formData.bigqueryTable.trim() && !formData.bigqueryQuery.trim()) {
        newErrors.bigqueryTable = "Either table or query is required.";
      }
    } else if (
      st === String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE)
    ) {
      if (!formData.snowflakeTable.trim()) {
        newErrors.snowflakeTable = "Table name is required for Snowflake.";
      }
    } else if (st === String(feast.core.DataSource.SourceType.STREAM_KAFKA)) {
      if (!formData.kafkaBootstrapServers.trim()) {
        newErrors.kafkaBootstrapServers = "Bootstrap servers are required.";
      }
      if (!formData.kafkaTopic.trim()) {
        newErrors.kafkaTopic = "Topic is required.";
      }
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
          helpText="s3://path/to/file, gs://path/to/file, or file:///local/path"
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
            helpText="Alternative to table: a SQL query"
          >
            <EuiFieldText
              value={formData.bigqueryQuery}
              onChange={(e) => updateField("bigqueryQuery", e.target.value)}
              placeholder="SELECT * FROM ..."
            />
          </EuiFormRow>
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE)) {
      return (
        <>
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
          <EuiFormRow label="Database">
            <EuiFieldText
              value={formData.snowflakeDatabase}
              onChange={(e) => updateField("snowflakeDatabase", e.target.value)}
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
        </>
      );
    }

    if (st === String(feast.core.DataSource.SourceType.BATCH_REDSHIFT)) {
      return (
        <>
          <EuiFormRow label="Table">
            <EuiFieldText
              value={formData.redshiftTable}
              onChange={(e) => updateField("redshiftTable", e.target.value)}
              placeholder="my_table"
            />
          </EuiFormRow>
          <EuiFormRow label="Database">
            <EuiFieldText
              value={formData.redshiftDatabase}
              onChange={(e) => updateField("redshiftDatabase", e.target.value)}
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
          >
            <EuiFieldText
              value={formData.kafkaBootstrapServers}
              onChange={(e) =>
                updateField("kafkaBootstrapServers", e.target.value)
              }
              isInvalid={!!errors.kafkaBootstrapServers}
              placeholder="localhost:9092"
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
          <EuiFormRow label="Table">
            <EuiFieldText
              value={formData.sparkTable}
              onChange={(e) => updateField("sparkTable", e.target.value)}
              placeholder="catalog.database.table"
            />
          </EuiFormRow>
          <EuiFormRow label="Path" helpText="Alternative to table">
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

  const isBatchSource =
    formData.sourceType !==
      String(feast.core.DataSource.SourceType.STREAM_KAFKA) &&
    formData.sourceType !==
      String(feast.core.DataSource.SourceType.REQUEST_SOURCE) &&
    formData.sourceType !==
      String(feast.core.DataSource.SourceType.PUSH_SOURCE);

  return (
    <FormModal
      title={isEdit ? "Edit Data Source" : "Create Data Source"}
      submitLabel={isEdit ? "Update Data Source" : "Create Data Source"}
      onClose={onClose}
      onSubmit={handleSubmit}
      width={650}
    >
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

      <EuiFormRow label="Source Type" helpText="Type of the data source.">
        <EuiSelect
          options={SOURCE_TYPE_OPTIONS}
          value={formData.sourceType}
          onChange={(e) => updateField("sourceType", e.target.value)}
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
            helpText="Event timestamp column name."
          >
            <EuiFieldText
              value={formData.timestampField}
              onChange={(e) => updateField("timestampField", e.target.value)}
              placeholder="event_timestamp"
            />
          </EuiFormRow>
          <EuiFormRow
            label="Created Timestamp Column"
            helpText="Column tracking when the row was created."
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
