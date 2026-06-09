import React, { useState, useEffect } from "react";
import {
  EuiFormRow,
  EuiFieldText,
  EuiFieldNumber,
  EuiSelect,
  EuiSwitch,
  EuiComboBox,
  EuiComboBoxOptionOption,
  EuiSpacer,
  EuiCallOut,
  EuiButtonEmpty,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import FormModal from "./forms/FormModal";
import TagsEditor, { TagEntry } from "./forms/TagsEditor";
import NameDescriptionOwnerFields from "./forms/NameDescriptionOwnerFields";
import FeatureFieldEditor, {
  FeatureFieldEntry,
} from "./forms/FeatureFieldEditor";
import EntityFormModal, { EntityFormData } from "./EntityFormModal";
import DataSourceFormModal, { DataSourceFormData } from "./DataSourceFormModal";
import { useLoadEntitiesREST } from "../queries/useLoadEntitiesREST";
import { useLoadDataSourcesREST } from "../queries/useLoadDataSourcesREST";
import { useApplyEntity } from "../queries/mutations/useEntityMutations";
import { useApplyDataSource } from "../queries/mutations/useDataSourceMutations";
import { feast } from "../protos";

const TTL_UNIT_OPTIONS = [
  { value: "seconds", text: "Seconds" },
  { value: "minutes", text: "Minutes" },
  { value: "hours", text: "Hours" },
  { value: "days", text: "Days" },
];

interface FeatureViewFormData {
  name: string;
  description: string;
  owner: string;
  entities: string[];
  features: FeatureFieldEntry[];
  batchSource: string;
  ttlValue: number;
  ttlUnit: string;
  online: boolean;
  tags: TagEntry[];
}

interface FeatureViewFormModalProps {
  onClose: () => void;
  onSubmit: (data: FeatureViewFormData) => void;
  initialData?: FeatureViewFormData;
  isEdit?: boolean;
}

const EMPTY_FORM: FeatureViewFormData = {
  name: "",
  description: "",
  owner: "",
  entities: [],
  features: [],
  batchSource: "",
  ttlValue: 0,
  ttlUnit: "seconds",
  online: true,
  tags: [],
};

const FeatureViewFormModal: React.FC<FeatureViewFormModalProps> = ({
  onClose,
  onSubmit,
  initialData,
  isEdit = false,
}) => {
  const [formData, setFormData] = useState<FeatureViewFormData>(
    initialData || EMPTY_FORM,
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState(false);
  const [showEntityForm, setShowEntityForm] = useState(false);
  const [showDataSourceForm, setShowDataSourceForm] = useState(false);

  const { projectName } = useParams();
  const entitiesQuery = useLoadEntitiesREST(projectName || "");
  const dataSourcesQuery = useLoadDataSourcesREST(projectName || "");
  const applyEntity = useApplyEntity();
  const applyDataSource = useApplyDataSource();

  const entities = entitiesQuery.data?.entities || [];
  const dataSources = dataSourcesQuery.data?.dataSources || [];

  const entityOptions: EuiComboBoxOptionOption<string>[] = entities.map(
    (e: any) => ({
      label: e?.spec?.name || e?.name || "",
    }),
  );

  const dataSourceOptions = dataSources.map((ds: any) => ({
    value: ds?.name || ds?.spec?.name || "",
    text: ds?.name || ds?.spec?.name || "",
  }));

  const hasNoEntities = entitiesQuery.isSuccess && entities.length === 0;
  const hasNoDataSources =
    dataSourcesQuery.isSuccess && dataSources.length === 0;

  useEffect(() => {
    if (initialData) {
      setFormData(initialData);
    }
  }, [initialData]);

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name.trim()) {
      newErrors.name = "Feature view name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(formData.name)) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, and underscores.";
    }

    if (formData.features.length === 0) {
      newErrors.features = "At least one feature is required.";
    } else {
      const hasEmptyName = formData.features.some((f) => !f.name.trim());
      if (hasEmptyName) {
        newErrors.features = "All features must have a name.";
      }
      const featureNames = formData.features.map((f) => f.name.trim());
      if (new Set(featureNames).size !== featureNames.length) {
        newErrors.features = "Feature names must be unique.";
      }
    }

    if (!formData.batchSource.trim()) {
      newErrors.batchSource = "A batch source is required.";
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

  const updateField = <K extends keyof FeatureViewFormData>(
    field: K,
    value: FeatureViewFormData[K],
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

  const handleInlineEntityCreate = (entityData: EntityFormData) => {
    const payload = {
      name: entityData.name,
      project: projectName || "",
      join_key: entityData.joinKeys[0] || entityData.name,
      value_type: parseInt(entityData.valueType, 10),
      description: entityData.description,
      tags: Object.fromEntries(
        entityData.tags
          .filter((t) => t.key.trim())
          .map((t) => [t.key, t.value]),
      ),
    };
    applyEntity.mutate(payload, {
      onSuccess: () => {
        setShowEntityForm(false);
        updateField("entities", [...formData.entities, entityData.name]);
      },
    });
  };

  const handleInlineDataSourceCreate = (dsData: DataSourceFormData) => {
    const payload: Record<string, any> = {
      name: dsData.name,
      project: projectName || "",
      type: parseInt(dsData.sourceType, 10),
      timestamp_field: dsData.timestampField,
      created_timestamp_column: dsData.createdTimestampColumn,
      description: dsData.description,
      owner: dsData.owner,
      tags: Object.fromEntries(
        dsData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
      ),
    };

    const st = dsData.sourceType;
    if (st === String(feast.core.DataSource.SourceType.BATCH_FILE)) {
      payload.file_options = { uri: dsData.fileUri };
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_BIGQUERY)) {
      payload.bigquery_options = {
        table: dsData.bigqueryTable,
        query: dsData.bigqueryQuery,
      };
    } else if (
      st === String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE)
    ) {
      payload.snowflake_options = {
        table: dsData.snowflakeTable,
        database: dsData.snowflakeDatabase,
        schema_: dsData.snowflakeSchema,
      };
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_REDSHIFT)) {
      payload.redshift_options = {
        table: dsData.redshiftTable,
        database: dsData.redshiftDatabase,
        schema_: dsData.redshiftSchema,
      };
    } else if (st === String(feast.core.DataSource.SourceType.STREAM_KAFKA)) {
      payload.kafka_options = {
        kafka_bootstrap_servers: dsData.kafkaBootstrapServers,
        topic: dsData.kafkaTopic,
      };
    } else if (st === String(feast.core.DataSource.SourceType.BATCH_SPARK)) {
      payload.spark_options = {
        table: dsData.sparkTable,
        path: dsData.sparkPath,
      };
    }

    applyDataSource.mutate(payload as any, {
      onSuccess: () => {
        setShowDataSourceForm(false);
        updateField("batchSource", dsData.name);
      },
    });
  };

  const selectedEntityOptions = formData.entities.map((e) => ({ label: e }));

  return (
    <>
      <FormModal
        title={isEdit ? "Edit Feature View" : "Create Feature View"}
        submitLabel={isEdit ? "Update Feature View" : "Create Feature View"}
        onClose={onClose}
        onSubmit={handleSubmit}
        width={750}
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
          nameHelpText="A unique name for this feature view."
          namePlaceholder="e.g. customer_features"
          descriptionPlaceholder="Describe what this feature view provides..."
        />

        {hasNoEntities && (
          <>
            <EuiSpacer size="s" />
            <EuiCallOut
              title="No entities found in this project."
              color="warning"
              iconType="alert"
              size="s"
            >
              <p>
                Feature views typically reference entities. You can create one
                now.
              </p>
              <EuiButtonEmpty
                size="s"
                iconType="plus"
                onClick={() => setShowEntityForm(true)}
              >
                Create Entity
              </EuiButtonEmpty>
            </EuiCallOut>
            <EuiSpacer size="s" />
          </>
        )}

        <EuiFormRow
          label="Entities"
          helpText="Entities that this feature view is associated with."
        >
          <EuiComboBox
            placeholder="Select entities"
            options={entityOptions}
            selectedOptions={selectedEntityOptions}
            onChange={(selected) =>
              updateField(
                "entities",
                selected.map((s) => s.label),
              )
            }
            isClearable
            isLoading={entitiesQuery.isLoading}
          />
        </EuiFormRow>

        {hasNoDataSources && (
          <>
            <EuiSpacer size="s" />
            <EuiCallOut
              title="No data sources found in this project."
              color="warning"
              iconType="alert"
              size="s"
            >
              <p>
                A batch source is required. You can create a data source now.
              </p>
              <EuiButtonEmpty
                size="s"
                iconType="plus"
                onClick={() => setShowDataSourceForm(true)}
              >
                Create Data Source
              </EuiButtonEmpty>
            </EuiCallOut>
            <EuiSpacer size="s" />
          </>
        )}

        <EuiFormRow
          label="Batch Source"
          isInvalid={!!errors.batchSource}
          error={errors.batchSource}
          helpText="The data source providing batch/offline features."
        >
          {dataSourceOptions.length > 0 ? (
            <EuiSelect
              options={[
                { value: "", text: "-- Select a data source --" },
                ...dataSourceOptions,
              ]}
              value={formData.batchSource}
              onChange={(e) => updateField("batchSource", e.target.value)}
              isInvalid={!!errors.batchSource}
            />
          ) : (
            <EuiFieldText
              value={formData.batchSource}
              onChange={(e) => updateField("batchSource", e.target.value)}
              isInvalid={!!errors.batchSource}
              placeholder="data_source_name"
            />
          )}
        </EuiFormRow>

        <EuiSpacer size="m" />

        <FeatureFieldEditor
          features={formData.features}
          onChange={(features) => updateField("features", features)}
          error={errors.features}
        />

        <EuiSpacer size="m" />

        <EuiFormRow
          label="TTL (Time to Live)"
          helpText="How long features remain valid after their event timestamp."
        >
          <div style={{ display: "flex", gap: 8 }}>
            <EuiFieldNumber
              value={formData.ttlValue}
              onChange={(e) =>
                updateField("ttlValue", parseInt(e.target.value) || 0)
              }
              min={0}
              style={{ width: 120 }}
            />
            <EuiSelect
              options={TTL_UNIT_OPTIONS}
              value={formData.ttlUnit}
              onChange={(e) => updateField("ttlUnit", e.target.value)}
              style={{ width: 140 }}
            />
          </div>
        </EuiFormRow>

        <EuiFormRow label="Online serving">
          <EuiSwitch
            label="Serve features online"
            checked={formData.online}
            onChange={(e) => updateField("online", e.target.checked)}
          />
        </EuiFormRow>

        <EuiSpacer size="m" />

        <TagsEditor
          tags={formData.tags}
          onChange={(tags) => updateField("tags", tags)}
          error={errors.tags}
        />
      </FormModal>

      {showEntityForm && (
        <EntityFormModal
          onClose={() => setShowEntityForm(false)}
          onSubmit={handleInlineEntityCreate}
        />
      )}

      {showDataSourceForm && (
        <DataSourceFormModal
          onClose={() => setShowDataSourceForm(false)}
          onSubmit={handleInlineDataSourceCreate}
        />
      )}
    </>
  );
};

export default FeatureViewFormModal;
export type { FeatureViewFormData };
