import React, { useState, useMemo } from "react";
import {
  EuiFormRow,
  EuiFieldText,
  EuiSpacer,
  EuiHorizontalRule,
  EuiText,
  EuiCallOut,
  EuiFlexGroup,
  EuiFlexItem,
  EuiPanel,
  EuiTitle,
  EuiComboBox,
  EuiComboBoxOptionOption,
  EuiCheckbox,
  EuiSuperSelect,
  EuiSuperSelectOption,
  EuiButton,
  EuiButtonEmpty,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import FormModal from "../../components/forms/FormModal";
import TagsEditor, { TagEntry } from "../../components/forms/TagsEditor";
import useResourceQuery, {
  featureServiceListPath,
  featureViewListPath,
  dataSourceListPath,
} from "../../queries/useResourceQuery";

export interface RegisterDatasetPayload {
  name: string;
  project: string;
  features: string[];
  join_keys: string[];
  storage_path: string;
  storage_type: string;
  tags: Record<string, string>;
  full_feature_names: boolean;
  feature_service_name?: string;
  namespace?: string;
  collection?: string;
  description?: string;
}

interface RegisterDatasetModalProps {
  onClose: () => void;
  onSubmit: (data: RegisterDatasetPayload) => Promise<void>;
  isSubmitting: boolean;
  error?: string | null;
  embedded?: boolean;
}

interface StorageTypeDefinition {
  value: string;
  label: string;
  description: string;
  placeholder: string;
  helpText: string;
  sourceTypeMatch: string[];
}

const ALL_STORAGE_TYPES: StorageTypeDefinition[] = [
  {
    value: "file",
    label: "File (Parquet / CSV)",
    description: "Local or remote file path (S3, GCS, HDFS)",
    placeholder: "s3://my-bucket/datasets/training_v1.parquet",
    helpText:
      "Path to the data file accessible by the Feast server (e.g. s3://bucket/path/data.parquet, gs://bucket/data.csv).",
    sourceTypeMatch: ["BATCH_FILE"],
  },
  {
    value: "bigquery",
    label: "BigQuery",
    description: "Google BigQuery table reference",
    placeholder: "project_id.dataset.table_name",
    helpText: "Full BigQuery table reference: project:dataset.table",
    sourceTypeMatch: ["BATCH_BIGQUERY"],
  },
  {
    value: "snowflake",
    label: "Snowflake",
    description: "Snowflake table reference",
    placeholder: "database.schema.table_name",
    helpText: "Snowflake table: database.schema.table",
    sourceTypeMatch: ["BATCH_SNOWFLAKE"],
  },
  {
    value: "redshift",
    label: "Redshift",
    description: "Amazon Redshift table reference",
    placeholder: "schema.table_name",
    helpText: "Redshift table: schema.table",
    sourceTypeMatch: ["BATCH_REDSHIFT"],
  },
  {
    value: "spark",
    label: "Spark",
    description: "Apache Spark table or path",
    placeholder: "s3://bucket/path/ or catalog.database.table",
    helpText:
      "Spark path or catalog table reference (the data will be read via Spark).",
    sourceTypeMatch: ["BATCH_SPARK"],
  },
  {
    value: "trino",
    label: "Trino",
    description: "Trino table reference",
    placeholder: "catalog.schema.table",
    helpText: "Trino table: catalog.schema.table",
    sourceTypeMatch: ["BATCH_TRINO"],
  },
  {
    value: "athena",
    label: "AWS Athena",
    description: "AWS Athena table reference",
    placeholder: "database.table_name",
    helpText: "Athena table reference. Data is queried via Athena.",
    sourceTypeMatch: ["BATCH_ATHENA"],
  },
  {
    value: "custom",
    label: "Custom",
    description: "Custom storage configuration",
    placeholder: '{"class": "my.CustomStorage", "config": {}}',
    helpText: "Serialized configuration for a custom storage implementation.",
    sourceTypeMatch: ["CUSTOM_SOURCE"],
  },
];

function detectDataSourceTypes(dataSources: any[]): Set<string> {
  const types = new Set<string>();
  for (const ds of dataSources) {
    const dsType = ds.spec?.type || ds.type;
    if (dsType != null) {
      const typeName = dataSourceTypeToName(dsType);
      if (typeName) types.add(typeName);
    }
    if (ds.spec?.fileOptions || ds.fileOptions) types.add("BATCH_FILE");
    if (ds.spec?.bigqueryOptions || ds.bigqueryOptions)
      types.add("BATCH_BIGQUERY");
    if (ds.spec?.snowflakeOptions || ds.snowflakeOptions)
      types.add("BATCH_SNOWFLAKE");
    if (ds.spec?.redshiftOptions || ds.redshiftOptions)
      types.add("BATCH_REDSHIFT");
    if (ds.spec?.sparkOptions || ds.sparkOptions) types.add("BATCH_SPARK");
    if (ds.spec?.trinoOptions || ds.trinoOptions) types.add("BATCH_TRINO");
    if (ds.spec?.athenaOptions || ds.athenaOptions) types.add("BATCH_ATHENA");
    if (ds.spec?.customOptions || ds.customOptions) types.add("CUSTOM_SOURCE");
  }
  return types;
}

function dataSourceTypeToName(typeNum: number | string): string | null {
  const map: Record<string, string> = {
    "1": "BATCH_FILE",
    "2": "BATCH_BIGQUERY",
    "3": "BATCH_REDSHIFT",
    "5": "BATCH_SNOWFLAKE",
    "7": "BATCH_SPARK",
    "8": "BATCH_TRINO",
    "9": "BATCH_ATHENA",
    "6": "STREAM_KAFKA",
    "10": "STREAM_KINESIS",
    "4": "REQUEST_SOURCE",
    "12": "PUSH_SOURCE",
    "11": "CUSTOM_SOURCE",
  };
  return map[String(typeNum)] || null;
}

const RegisterDatasetModal = ({
  onClose,
  onSubmit,
  isSubmitting,
  error,
  embedded = false,
}: RegisterDatasetModalProps) => {
  const { projectName } = useParams();

  const { data: featureServicesRaw } = useResourceQuery<any[]>({
    resourceType: "register-modal-fs",
    project: projectName,
    restPath: featureServiceListPath(projectName),
    restSelect: (d) => d.featureServices || [],
  });

  const { data: featureViewsRaw } = useResourceQuery<any[]>({
    resourceType: "register-modal-fv",
    project: projectName,
    restPath: featureViewListPath(projectName),
    restSelect: (d) => d.featureViews || [],
  });

  const { data: dataSourcesRaw } = useResourceQuery<any[]>({
    resourceType: "register-modal-ds",
    project: projectName,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources || [],
  });

  // Derive available storage types from project's data sources
  const availableStorageOptions: EuiSuperSelectOption<string>[] =
    useMemo(() => {
      if (!dataSourcesRaw || dataSourcesRaw.length === 0) {
        // Fallback: show all storage types if no data sources loaded yet
        return ALL_STORAGE_TYPES.map((st) => ({
          value: st.value,
          inputDisplay: st.label,
          dropdownDisplay: (
            <>
              <strong>{st.label}</strong>
              <EuiText size="xs" color="subdued">
                <p>{st.description}</p>
              </EuiText>
            </>
          ),
        }));
      }

      const detectedTypes = detectDataSourceTypes(dataSourcesRaw);

      const matched = ALL_STORAGE_TYPES.filter((st) =>
        st.sourceTypeMatch.some((match) => detectedTypes.has(match)),
      );

      // Always include File as a fallback (datasets can be stored as standalone files)
      const hasFile = matched.some((st) => st.value === "file");
      const result = hasFile ? matched : [ALL_STORAGE_TYPES[0], ...matched];

      return result.map((st) => ({
        value: st.value,
        inputDisplay: st.label,
        dropdownDisplay: (
          <>
            <strong>{st.label}</strong>
            <EuiText size="xs" color="subdued">
              <p>{st.description}</p>
            </EuiText>
          </>
        ),
      }));
    }, [dataSourcesRaw]);

  // Form state
  const [name, setName] = useState("");
  const [namespace, setNamespace] = useState("");
  const [collection, setCollection] = useState("");
  const [description, setDescription] = useState("");
  const [storagePath, setStoragePath] = useState("");
  const [storageType, setStorageType] = useState("file");
  const [featuresInput, setFeaturesInput] = useState<EuiComboBoxOptionOption[]>(
    [],
  );
  const [joinKeysInput, setJoinKeysInput] = useState<EuiComboBoxOptionOption[]>(
    [],
  );
  const [tags, setTags] = useState<TagEntry[]>([]);
  const [featureServiceName, setFeatureServiceName] = useState("");
  const [fullFeatureNames, setFullFeatureNames] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState(false);

  // Get current storage type config
  const currentStorageConfig =
    ALL_STORAGE_TYPES.find((st) => st.value === storageType) ||
    ALL_STORAGE_TYPES[0];

  // Build suggestions from live data
  const featureOptions: EuiComboBoxOptionOption[] = (featureViewsRaw || [])
    .filter((fv: any) => fv.type !== "labelView")
    .flatMap((fv: any) => {
      const fvName = fv.spec?.name || "";
      const features = fv.spec?.features || [];
      return features.map((f: any) => ({
        label: `${fvName}:${f.name || f}`,
      }));
    });

  const joinKeyOptions: EuiComboBoxOptionOption[] = (() => {
    const seen = new Set<string>();
    (featureViewsRaw || []).forEach((fv: any) => {
      const entities = fv.spec?.entities || [];
      entities.forEach((e: string) => {
        if (!seen.has(e)) seen.add(e);
      });
    });
    return Array.from(seen).map((k) => ({ label: k }));
  })();

  const featureServiceOptions: EuiComboBoxOptionOption[] = (
    featureServicesRaw || []
  ).map((fs: any) => ({
    label: fs.spec?.name || fs.name || "",
  }));

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!name.trim()) {
      newErrors.name = "Dataset name is required.";
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_-]*$/.test(name.trim())) {
      newErrors.name =
        "Must start with a letter or underscore, and contain only letters, numbers, underscores, and hyphens.";
    }

    if (!storagePath.trim()) {
      newErrors.storagePath =
        "Storage path is required. Provide a file path or table reference.";
    } else if (storageType === "file") {
      if (
        !/^(s3|gs|gcs|hdfs|abfs|file):\/\/\S+$/.test(storagePath.trim()) &&
        !storagePath.trim().startsWith("/")
      ) {
        newErrors.storagePath =
          "Should be a valid URI (s3://, gs://, hdfs://, file://) or absolute path.";
      }
    }

    const tagKeys = tags.map((t) => t.key).filter((k) => k.trim());
    if (new Set(tagKeys).size !== tagKeys.length) {
      newErrors.tags = "Tag keys must be unique.";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async () => {
    setSubmitted(true);
    if (!validate()) return;

    const tagsObj: Record<string, string> = {};
    tags.forEach(({ key, value }) => {
      if (key.trim() && value.trim()) tagsObj[key.trim()] = value.trim();
    });

    const payload: RegisterDatasetPayload = {
      name: name.trim(),
      project: projectName || "",
      features: featuresInput.map((o) => o.label),
      join_keys: joinKeysInput.map((o) => o.label),
      storage_path: storagePath.trim(),
      storage_type: storageType,
      tags: tagsObj,
      full_feature_names: fullFeatureNames,
      feature_service_name: featureServiceName || undefined,
      namespace: namespace.trim() || undefined,
      collection: collection.trim() || undefined,
      description: description.trim() || undefined,
    };
    await onSubmit(payload);
  };

  const clearFieldError = (field: string) => {
    if (submitted) {
      setErrors((prev) => {
        const next = { ...prev };
        delete next[field];
        return next;
      });
    }
  };

  const formContent = (
    <>
      {error && (
        <>
          <EuiCallOut
            title="Registration failed"
            color="danger"
            iconType="alert"
            size="s"
          >
            <p>{error}</p>
          </EuiCallOut>
          <EuiSpacer size="m" />
        </>
      )}

      {/* Section: Identity */}
      <EuiTitle size="xxs">
        <h4>Identity</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiFlexGroup gutterSize="m">
        <EuiFlexItem>
          <EuiFormRow
            label="Dataset Name"
            isInvalid={!!errors.name}
            error={errors.name}
            helpText="Unique identifier for this dataset within the project."
          >
            <EuiFieldText
              value={name}
              onChange={(e) => {
                setName(e.target.value);
                clearFieldError("name");
              }}
              isInvalid={!!errors.name}
              placeholder="e.g. driver_training_v1"
              autoFocus
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiFormRow
            label="Description"
            helpText="Brief description of this dataset."
          >
            <EuiFieldText
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="e.g. Training data for driver fraud model"
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiFormRow
            label="Feature Service (optional)"
            helpText="Associate with a feature service for lineage."
          >
            <EuiComboBox
              singleSelection={{ asPlainText: true }}
              options={featureServiceOptions}
              selectedOptions={
                featureServiceName ? [{ label: featureServiceName }] : []
              }
              onChange={(selected) =>
                setFeatureServiceName(
                  selected.length > 0 ? selected[0].label : "",
                )
              }
              onCreateOption={(val) => setFeatureServiceName(val)}
              placeholder="Select or type..."
              isClearable
            />
          </EuiFormRow>
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />

      {/* Section: Organization */}
      <EuiTitle size="xxs">
        <h4>Organization (optional)</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiPanel color="subdued" paddingSize="s" hasBorder={false}>
        <EuiText size="xs" color="subdued">
          Group datasets into namespaces and collections for hierarchical
          organization. Leave empty to keep the dataset at the top level.
        </EuiText>
      </EuiPanel>
      <EuiSpacer size="s" />

      <EuiFlexGroup gutterSize="m">
        <EuiFlexItem>
          <EuiFormRow
            label="Namespace"
            helpText="Top-level grouping (e.g. fraud, underwriting, analytics)."
          >
            <EuiFieldText
              value={namespace}
              onChange={(e) => setNamespace(e.target.value)}
              placeholder="e.g. fraud"
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiFormRow
            label="Collection"
            helpText="Sub-grouping within the namespace (e.g. raw, curated, training)."
          >
            <EuiFieldText
              value={collection}
              onChange={(e) => setCollection(e.target.value)}
              placeholder="e.g. training"
            />
          </EuiFormRow>
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />

      {/* Section: Storage */}
      <EuiTitle size="xxs">
        <h4>Storage Location</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiPanel color="subdued" paddingSize="s" hasBorder={false}>
        <EuiText size="xs" color="subdued">
          Point to where the dataset data already exists. Only storage types
          matching your project's configured data sources are shown.
        </EuiText>
      </EuiPanel>
      <EuiSpacer size="m" />

      <EuiFormRow label="Storage Type">
        <EuiSuperSelect
          options={availableStorageOptions}
          valueOfSelected={storageType}
          onChange={(value) => setStorageType(value)}
          fullWidth
        />
      </EuiFormRow>

      <EuiFormRow
        label="Path / Table Reference"
        isInvalid={!!errors.storagePath}
        error={errors.storagePath}
        helpText={currentStorageConfig.helpText}
      >
        <EuiFieldText
          value={storagePath}
          onChange={(e) => {
            setStoragePath(e.target.value);
            clearFieldError("storagePath");
          }}
          isInvalid={!!errors.storagePath}
          placeholder={currentStorageConfig.placeholder}
          icon={storageType === "file" ? "document" : "storage"}
          fullWidth
        />
      </EuiFormRow>

      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />

      {/* Section: Schema */}
      <EuiTitle size="xxs">
        <h4>Schema</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiFormRow
        label="Features"
        helpText="Select from existing features or type custom ones (feature_view:feature_name)."
      >
        <EuiComboBox
          options={featureOptions}
          selectedOptions={featuresInput}
          onCreateOption={(val) => {
            setFeaturesInput([...featuresInput, { label: val }]);
          }}
          onChange={(selected) => setFeaturesInput(selected)}
          placeholder="Search or type features..."
          isClearable
          fullWidth
        />
      </EuiFormRow>

      <EuiFormRow
        label="Join Keys"
        helpText="Entity columns used to join this dataset with other data."
      >
        <EuiComboBox
          options={joinKeyOptions}
          selectedOptions={joinKeysInput}
          onCreateOption={(val) => {
            setJoinKeysInput([...joinKeysInput, { label: val }]);
          }}
          onChange={(selected) => setJoinKeysInput(selected)}
          placeholder="Search or type join keys..."
          isClearable
          fullWidth
        />
      </EuiFormRow>

      <EuiSpacer size="s" />
      <EuiCheckbox
        id="full-feature-names-checkbox"
        label="Use full feature names (feature_view__feature_name format)"
        checked={fullFeatureNames}
        onChange={(e) => setFullFeatureNames(e.target.checked)}
      />

      {/* Section: Tags */}
      <EuiSpacer size="m" />
      <TagsEditor
        tags={tags}
        onChange={(newTags) => setTags(newTags)}
        error={errors.tags}
      />
    </>
  );

  if (embedded) {
    return (
      <div style={{ paddingTop: 16 }}>
        {formContent}
        <EuiSpacer size="l" />
        <EuiFlexGroup justifyContent="flexEnd" gutterSize="s">
          <EuiFlexItem grow={false}>
            <EuiButtonEmpty onClick={onClose}>Cancel</EuiButtonEmpty>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButton
              fill
              onClick={handleSubmit}
              isLoading={isSubmitting}
              iconType="link"
            >
              Link Existing Dataset
            </EuiButton>
          </EuiFlexItem>
        </EuiFlexGroup>
      </div>
    );
  }

  return (
    <FormModal
      title="Register Dataset"
      submitLabel="Register Dataset"
      onClose={onClose}
      onSubmit={handleSubmit}
      width={720}
      isSubmitting={isSubmitting}
    >
      {formContent}
    </FormModal>
  );
};

export default RegisterDatasetModal;
export type { RegisterDatasetModalProps };
