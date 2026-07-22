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
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import FormModal from "../../components/forms/FormModal";
import TagsEditor, { TagEntry } from "../../components/forms/TagsEditor";
import useResourceQuery, {
  featureServiceListPath,
  featureViewListPath,
  dataSourceListPath,
} from "../../queries/useResourceQuery";

interface EditDatasetModalProps {
  dataset: any;
  onClose: () => void;
  onSubmit: (data: any) => Promise<void>;
  isSubmitting: boolean;
  error?: string | null;
}

interface StorageTypeDef {
  value: string;
  label: string;
  description: string;
  placeholder: string;
  helpText: string;
  sourceTypeMatch: string[];
}

const ALL_STORAGE_TYPES: StorageTypeDef[] = [
  {
    value: "file",
    label: "File (Parquet / CSV)",
    description: "Local or remote file path (S3, GCS, HDFS)",
    placeholder: "s3://my-bucket/datasets/training_v1.parquet",
    helpText: "Path to the data file accessible by the Feast server.",
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
    helpText: "Spark path or catalog table reference.",
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
    helpText: "Athena table reference.",
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

function detectStorageType(dataset: any): string {
  const storage = dataset?.spec?.storage;
  if (!storage) return "file";
  if (storage.fileStorage) return "file";
  if (storage.bigqueryStorage) return "bigquery";
  if (storage.snowflakeStorage) return "snowflake";
  if (storage.redshiftStorage) return "redshift";
  if (storage.sparkStorage) return "spark";
  if (storage.trinoStorage) return "trino";
  if (storage.athenaStorage) return "athena";
  if (storage.customStorage) return "custom";
  return "file";
}

function extractStoragePath(dataset: any): string {
  const storage = dataset?.spec?.storage;
  if (!storage) return "";
  if (storage.fileStorage?.uri) return storage.fileStorage.uri;
  if (storage.bigqueryStorage?.table) return storage.bigqueryStorage.table;
  if (storage.snowflakeStorage?.table) return storage.snowflakeStorage.table;
  if (storage.redshiftStorage?.table) return storage.redshiftStorage.table;
  if (storage.sparkStorage?.path) return storage.sparkStorage.path;
  if (storage.sparkStorage?.table) return storage.sparkStorage.table;
  if (storage.trinoStorage?.table) return storage.trinoStorage.table;
  if (storage.athenaStorage?.table) return storage.athenaStorage.table;
  if (storage.customStorage?.configuration)
    return storage.customStorage.configuration;
  return "";
}

const EditDatasetModal = ({
  dataset,
  onClose,
  onSubmit,
  isSubmitting,
  error,
}: EditDatasetModalProps) => {
  const { projectName } = useParams();
  const spec = dataset?.spec || {};
  const datasetName = spec.name || "";

  // Load data sources to filter storage type options
  const { data: dataSourcesRaw } = useResourceQuery<any[]>({
    resourceType: "edit-modal-ds",
    project: projectName,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources || [],
  });

  // Load feature services for dropdown
  const { data: featureServicesRaw } = useResourceQuery<any[]>({
    resourceType: "edit-modal-fs",
    project: projectName,
    restPath: featureServiceListPath(projectName),
    restSelect: (d) => d.featureServices || [],
  });

  // Load feature views for features/join key suggestions
  const { data: featureViewsRaw } = useResourceQuery<any[]>({
    resourceType: "edit-modal-fv",
    project: projectName,
    restPath: featureViewListPath(projectName),
    restSelect: (d) => d.featureViews || [],
  });

  // Derive available storage types from project's data sources
  const availableStorageOptions: EuiSuperSelectOption<string>[] =
    useMemo(() => {
      const currentType = detectStorageType(dataset);

      if (!dataSourcesRaw || dataSourcesRaw.length === 0) {
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

      let matched = ALL_STORAGE_TYPES.filter((st) =>
        st.sourceTypeMatch.some((match) => detectedTypes.has(match)),
      );

      // Always include File as a fallback
      if (!matched.some((st) => st.value === "file")) {
        matched = [ALL_STORAGE_TYPES[0], ...matched];
      }

      // Always include the dataset's current storage type so it's visible
      if (!matched.some((st) => st.value === currentType)) {
        const currentDef = ALL_STORAGE_TYPES.find(
          (st) => st.value === currentType,
        );
        if (currentDef) matched = [currentDef, ...matched];
      }

      return matched.map((st) => ({
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
    }, [dataSourcesRaw, dataset]);

  // Build feature suggestions from loaded feature views
  const featureOptions: EuiComboBoxOptionOption[] = useMemo(
    () =>
      (featureViewsRaw || [])
        .filter((fv: any) => fv.type !== "labelView")
        .flatMap((fv: any) => {
          const fvName = fv.spec?.name || "";
          const features = fv.spec?.features || [];
          return features.map((f: any) => ({
            label: `${fvName}:${f.name || f}`,
          }));
        }),
    [featureViewsRaw],
  );

  // Build join key suggestions from feature views' entities
  const joinKeyOptions: EuiComboBoxOptionOption[] = useMemo(() => {
    const seen = new Set<string>();
    (featureViewsRaw || []).forEach((fv: any) => {
      const entities = fv.spec?.entities || [];
      entities.forEach((e: string) => {
        if (!seen.has(e)) seen.add(e);
      });
    });
    return Array.from(seen).map((k) => ({ label: k }));
  }, [featureViewsRaw]);

  // Build feature service options for dropdown
  const featureServiceOptions: EuiComboBoxOptionOption[] = useMemo(
    () =>
      (featureServicesRaw || []).map((fs: any) => ({
        label: fs.spec?.name || fs.name || "",
      })),
    [featureServicesRaw],
  );

  // Form state
  const [storagePath, setStoragePath] = useState(extractStoragePath(dataset));
  const [storageType, setStorageType] = useState(detectStorageType(dataset));
  const [namespace, setNamespace] = useState(spec.namespace || "");
  const [collection, setCollection] = useState(spec.collection || "");
  const [description, setDescription] = useState(spec.description || "");
  const [featuresInput, setFeaturesInput] = useState<EuiComboBoxOptionOption[]>(
    (spec.features || []).map((f: string) => ({ label: f })),
  );
  const [joinKeysInput, setJoinKeysInput] = useState<EuiComboBoxOptionOption[]>(
    (spec.joinKeys || spec.join_keys || []).map((k: string) => ({ label: k })),
  );
  const [tags, setTags] = useState<TagEntry[]>(
    Object.entries(spec.tags || {}).map(([key, value]) => ({
      key,
      value: value as string,
    })),
  );
  const [featureServiceName, setFeatureServiceName] = useState(
    spec.featureServiceName || spec.feature_service_name || "",
  );
  const [fullFeatureNames, setFullFeatureNames] = useState(
    spec.fullFeatureNames || spec.full_feature_names || false,
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState(false);

  // Get current storage type config
  const currentStorageConfig =
    ALL_STORAGE_TYPES.find((st) => st.value === storageType) ||
    ALL_STORAGE_TYPES[0];

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!storagePath.trim()) {
      newErrors.storagePath = "Storage path is required.";
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

    const payload = {
      name: datasetName,
      project: "",
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
      allow_override: true,
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

  return (
    <FormModal
      title={`Edit Dataset: ${datasetName}`}
      submitLabel="Save Changes"
      onClose={onClose}
      onSubmit={handleSubmit}
      width={720}
      isSubmitting={isSubmitting}
    >
      {error && (
        <>
          <EuiCallOut
            title="Update failed"
            color="danger"
            iconType="alert"
            size="s"
          >
            <p>{error}</p>
          </EuiCallOut>
          <EuiSpacer size="m" />
        </>
      )}

      {/* Identity (read-only name) */}
      <EuiTitle size="xxs">
        <h4>Identity</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiFlexGroup gutterSize="m">
        <EuiFlexItem>
          <EuiFormRow
            label="Dataset Name"
            helpText="Name cannot be changed after creation."
          >
            <EuiFieldText value={datasetName} disabled />
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
      </EuiFlexGroup>
      <EuiFlexGroup gutterSize="m">
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

      {/* Organization */}
      <EuiTitle size="xxs">
        <h4>Organization (optional)</h4>
      </EuiTitle>
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

      {/* Storage */}
      <EuiTitle size="xxs">
        <h4>Storage Location</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiPanel color="subdued" paddingSize="s" hasBorder={false}>
        <EuiText size="xs" color="subdued">
          Only storage types matching your project's configured data sources are
          shown.
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

      {/* Schema */}
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
        id="edit-full-feature-names-checkbox"
        label="Use full feature names (feature_view__feature_name format)"
        checked={fullFeatureNames}
        onChange={(e) => setFullFeatureNames(e.target.checked)}
      />

      {/* Tags */}
      <EuiSpacer size="m" />
      <TagsEditor
        tags={tags}
        onChange={(newTags) => setTags(newTags)}
        error={errors.tags}
      />
    </FormModal>
  );
};

export default EditDatasetModal;
