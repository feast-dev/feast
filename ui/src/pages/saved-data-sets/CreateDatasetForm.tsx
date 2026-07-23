import React, { useState, useMemo, useCallback, useContext } from "react";
import {
  EuiSpacer,
  EuiFormRow,
  EuiFieldText,
  EuiRadioGroup,
  EuiComboBox,
  EuiComboBoxOptionOption,
  EuiButton,
  EuiButtonEmpty,
  EuiFlexGroup,
  EuiFlexItem,
  EuiText,
  EuiCallOut,
  EuiPanel,
  EuiTitle,
  EuiHorizontalRule,
  EuiSuperSelect,
  EuiSuperSelectOption,
  EuiDatePicker,
  EuiDatePickerRange,
  EuiTextArea,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import { useMutation, useQueryClient } from "react-query";
import moment, { Moment } from "moment";
import useResourceQuery, {
  featureServiceListPath,
  featureViewListPath,
  dataSourceListPath,
} from "../../queries/useResourceQuery";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { useDataMode } from "../../contexts/DataModeContext";
import { restPost } from "../../queries/restApiClient";
import TagsEditor, { TagEntry } from "../../components/forms/TagsEditor";
import JobStatusPanel from "./JobStatusPanel";

interface CreateDatasetFormProps {
  onClose: () => void;
}

const FEATURE_MODE_OPTIONS = [
  { id: "service", label: "Use a Feature Service" },
  { id: "individual", label: "Select individual features" },
];

const ENTITY_SOURCE_OPTIONS = [
  { id: "inline", label: "Define entity keys and time range manually" },
  { id: "reference", label: "Reference an existing data source path" },
];

const STORAGE_TYPES = [
  {
    value: "file",
    label: "File (Parquet)",
    placeholder: "s3://bucket/path/output.parquet",
  },
  {
    value: "bigquery",
    label: "BigQuery",
    placeholder: "project.dataset.table",
  },
  {
    value: "snowflake",
    label: "Snowflake",
    placeholder: "database.schema.table",
  },
  { value: "redshift", label: "Redshift", placeholder: "schema.table" },
  { value: "spark", label: "Spark", placeholder: "s3://bucket/path/" },
  { value: "trino", label: "Trino", placeholder: "catalog.schema.table" },
  { value: "athena", label: "Athena", placeholder: "database.table" },
  {
    value: "postgres",
    label: "PostgreSQL",
    placeholder: "schema.table_name",
  },
  {
    value: "clickhouse",
    label: "ClickHouse",
    placeholder: "database.table_name",
  },
  {
    value: "couchbase",
    label: "Couchbase Columnar",
    placeholder: "database.scope.collection",
  },
];

const CreateDatasetForm = ({ onClose }: CreateDatasetFormProps) => {
  const { projectName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();
  const queryClient = useQueryClient();

  // Step 1: Feature selection
  const [featureMode, setFeatureMode] = useState("service");
  const [selectedService, setSelectedService] = useState<
    EuiComboBoxOptionOption[]
  >([]);
  const [selectedFeatures, setSelectedFeatures] = useState<
    EuiComboBoxOptionOption[]
  >([]);

  // Step 2: Entity source
  const [entitySourceType, setEntitySourceType] = useState("inline");

  // Inline mode state
  const [entityKeys, setEntityKeys] = useState<EuiComboBoxOptionOption[]>([]);
  const [entityValues, setEntityValues] = useState("");
  const [startDate, setStartDate] = useState<Moment | null>(
    moment().subtract(30, "days"),
  );
  const [endDate, setEndDate] = useState<Moment | null>(moment());
  const [extraColumns, setExtraColumns] = useState("");

  // Reference mode state
  const [entitySourcePath, setEntitySourcePath] = useState("");

  // Step 3: Storage & metadata
  const [datasetName, setDatasetName] = useState("");
  const [namespace, setNamespace] = useState("");
  const [collection, setCollection] = useState("");
  const [description, setDescription] = useState("");
  const [storageType, setStorageType] = useState("file");
  const [storagePath, setStoragePath] = useState("");
  const [storageFileFormat, setStorageFileFormat] = useState("parquet");
  const [tags, setTags] = useState<TagEntry[]>([]);
  const [allowOverwrite] = useState(false);

  // Job tracking
  const [jobId, setJobId] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);

  // Fetch data for suggestions
  const { data: featureServicesRaw } = useResourceQuery<any[]>({
    resourceType: "create-dataset-fs",
    project: projectName,
    restPath: featureServiceListPath(projectName),
    restSelect: (d) => d.featureServices || [],
  });

  const { data: featureViewsRaw } = useResourceQuery<any[]>({
    resourceType: "create-dataset-fv",
    project: projectName,
    restPath: featureViewListPath(projectName),
    restSelect: (d) => d.featureViews || [],
  });

  const { data: dataSourcesRaw } = useResourceQuery<any[]>({
    resourceType: "create-dataset-ds",
    project: projectName,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources || [],
  });

  const featureServiceOptions: EuiComboBoxOptionOption[] = useMemo(
    () =>
      (featureServicesRaw || []).map((fs: any) => ({
        label: fs.spec?.name || fs.name || "",
      })),
    [featureServicesRaw],
  );

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

  // Extract entity/join key options from feature views
  const joinKeyOptions: EuiComboBoxOptionOption[] = useMemo(() => {
    const seen = new Set<string>();
    (featureViewsRaw || []).forEach((fv: any) => {
      const entities = fv.spec?.entities || [];
      entities.forEach((e: string) => {
        if (e && !seen.has(e)) seen.add(e);
      });
    });
    return Array.from(seen).map((k) => ({ label: k }));
  }, [featureViewsRaw]);

  // Filter storage types based on configured data sources
  const storageOptions: EuiSuperSelectOption<string>[] = useMemo(() => {
    if (!dataSourcesRaw || dataSourcesRaw.length === 0) {
      return STORAGE_TYPES.map((st) => ({
        value: st.value,
        inputDisplay: st.label,
        dropdownDisplay: <strong>{st.label}</strong>,
      }));
    }

    const detectedTypes = new Set<string>();
    for (const ds of dataSourcesRaw) {
      const spec = ds.spec || ds;
      if (spec.fileOptions || ds.fileOptions) detectedTypes.add("file");
      if (spec.bigqueryOptions || ds.bigqueryOptions)
        detectedTypes.add("bigquery");
      if (spec.snowflakeOptions || ds.snowflakeOptions)
        detectedTypes.add("snowflake");
      if (spec.redshiftOptions || ds.redshiftOptions)
        detectedTypes.add("redshift");
      if (spec.sparkOptions || ds.sparkOptions) detectedTypes.add("spark");
      if (spec.trinoOptions || ds.trinoOptions) detectedTypes.add("trino");
      if (spec.athenaOptions || ds.athenaOptions) detectedTypes.add("athena");
      const dsType = spec.type || ds.type;
      if (dsType === 1) detectedTypes.add("file");
      if (dsType === 2) detectedTypes.add("bigquery");
      if (dsType === 3) detectedTypes.add("redshift");
      if (dsType === 5) detectedTypes.add("snowflake");
      if (dsType === 7) detectedTypes.add("spark");
      if (dsType === 8) detectedTypes.add("trino");
      if (dsType === 9) detectedTypes.add("athena");
      const classType =
        spec.dataSourceClassType || ds.dataSourceClassType || "";
      if (classType.includes("postgres")) detectedTypes.add("postgres");
      if (classType.includes("clickhouse")) detectedTypes.add("clickhouse");
      if (classType.includes("couchbase")) detectedTypes.add("couchbase");
    }

    // Always include file as a fallback
    detectedTypes.add("file");

    const filtered = STORAGE_TYPES.filter((st) => detectedTypes.has(st.value));
    return filtered.map((st) => ({
      value: st.value,
      inputDisplay: st.label,
      dropdownDisplay: <strong>{st.label}</strong>,
    }));
  }, [dataSourcesRaw]);

  // Extract available data source paths for the reference entity source option
  const dataSourcePathOptions: EuiComboBoxOptionOption[] = useMemo(() => {
    if (!dataSourcesRaw) return [];
    const paths: EuiComboBoxOptionOption[] = [];
    for (const ds of dataSourcesRaw) {
      const spec = ds.spec || ds;
      const name = spec.name || ds.name || "";
      const fileOpts = spec.fileOptions || ds.fileOptions;
      const bqOpts = spec.bigqueryOptions || ds.bigqueryOptions;
      const sfOpts = spec.snowflakeOptions || ds.snowflakeOptions;
      const rsOpts = spec.redshiftOptions || ds.redshiftOptions;
      const sparkOpts = spec.sparkOptions || ds.sparkOptions;
      const trinoOpts = spec.trinoOptions || ds.trinoOptions;
      const athenaOpts = spec.athenaOptions || ds.athenaOptions;

      let path = "";
      if (fileOpts?.uri) path = fileOpts.uri;
      else if (fileOpts?.path) path = fileOpts.path;
      else if (bqOpts?.table) path = bqOpts.table;
      else if (sfOpts?.table) path = sfOpts.table;
      else if (rsOpts?.table) path = rsOpts.table;
      else if (sparkOpts?.path) path = sparkOpts.path;
      else if (sparkOpts?.table) path = sparkOpts.table;
      else if (trinoOpts?.table) path = trinoOpts.table;
      else if (athenaOpts?.table) path = athenaOpts.table;

      if (path) {
        paths.push({ label: path, key: name });
      }
    }
    return paths;
  }, [dataSourcesRaw]);

  const currentStorageType =
    STORAGE_TYPES.find((s) => s.value === storageType) || STORAGE_TYPES[0];

  // Submit create job
  const createMutation = useMutation(
    async () => {
      const payload: any = {
        name: datasetName.trim(),
        project: projectName || "",
        storage_type: storageType,
        storage_path: storagePath.trim(),
        storage_file_format:
          storageType === "spark" ? storageFileFormat : undefined,
        entity_source_type: entitySourceType,
        allow_overwrite: allowOverwrite,
        tags: tags.reduce(
          (acc, t) => {
            if (t.key.trim() && t.value.trim())
              acc[t.key.trim()] = t.value.trim();
            return acc;
          },
          {} as Record<string, string>,
        ),
      };

      if (namespace.trim()) payload.namespace = namespace.trim();
      if (collection.trim()) payload.collection = collection.trim();
      if (description.trim()) payload.description = description.trim();

      if (featureMode === "service" && selectedService.length > 0) {
        payload.feature_service_name = selectedService[0].label;
      } else if (featureMode === "individual" && selectedFeatures.length > 0) {
        payload.features = selectedFeatures.map((f) => f.label);
      }

      if (entitySourceType === "inline") {
        payload.entity_keys = entityKeys.map((k) => k.label);
        payload.entity_values = entityValues.trim();
        if (startDate) payload.start_date = startDate.toISOString();
        if (endDate) payload.end_date = endDate.toISOString();
      } else if (entitySourceType === "reference") {
        payload.entity_source_path = entitySourcePath.trim();
      }

      // Extra columns apply to all entity source methods
      if (extraColumns.trim()) {
        payload.extra_columns = extraColumns.trim();
      }

      return restPost(
        registryUrl,
        "/saved_datasets/create",
        payload,
        fetchOptions,
      );
    },
    {
      onSuccess: (data: any) => {
        setJobId(data.job_id);
        setSubmitError(null);
      },
      onError: (err: Error) => {
        setSubmitError(err.message);
      },
    },
  );

  const handleJobComplete = useCallback(() => {
    queryClient.invalidateQueries(["rest", "saved-datasets-list"]);
  }, [queryClient]);

  // Validation
  const canProceedStep0 =
    featureMode === "service"
      ? selectedService.length > 0
      : selectedFeatures.length > 0;

  const canProceedStep1 = (() => {
    if (entitySourceType === "inline") {
      return entityKeys.length > 0 && entityValues.trim().length > 0;
    }
    return entitySourcePath.trim().length > 0;
  })();

  const canSubmit =
    datasetName.trim().length > 0 &&
    storagePath.trim().length > 0 &&
    /^[a-zA-Z_][a-zA-Z0-9_-]*$/.test(datasetName.trim());

  const handleRetry = useCallback(() => {
    setJobId(null);
    setSubmitError(null);
  }, []);

  if (jobId) {
    return (
      <div style={{ paddingTop: 16 }}>
        <JobStatusPanel
          jobId={jobId}
          datasetName={datasetName}
          onComplete={handleJobComplete}
          onClose={onClose}
          onRetry={handleRetry}
        />
      </div>
    );
  }

  return (
    <div style={{ paddingTop: 16 }}>
      {submitError && (
        <>
          <EuiCallOut
            title="Failed to create dataset"
            color="danger"
            iconType="alert"
            size="s"
          >
            <p>{submitError}</p>
          </EuiCallOut>
          <EuiSpacer size="m" />
        </>
      )}

      <EuiPanel color="subdued" paddingSize="s" hasBorder={false}>
        <EuiText size="xs" color="subdued">
          Create a new dataset by running a feature retrieval job. Feast will
          execute get_historical_features, persist the results to your chosen
          storage, and register the dataset in the catalog.
        </EuiText>
      </EuiPanel>
      <EuiSpacer size="m" />

      {/* Step 1: Feature Selection */}
      <EuiTitle size="xxs">
        <h4>Step 1: Define Features</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiRadioGroup
        options={FEATURE_MODE_OPTIONS}
        idSelected={featureMode}
        onChange={(id) => setFeatureMode(id)}
      />
      <EuiSpacer size="m" />

      {featureMode === "service" ? (
        <EuiFormRow
          label="Feature Service"
          helpText="Select a pre-defined feature bundle."
        >
          <EuiComboBox
            singleSelection={{ asPlainText: true }}
            options={featureServiceOptions}
            selectedOptions={selectedService}
            onChange={setSelectedService}
            placeholder="Select a feature service..."
            isClearable
            fullWidth
          />
        </EuiFormRow>
      ) : (
        <EuiFormRow
          label="Features"
          helpText="Select individual features (feature_view:feature_name)."
        >
          <EuiComboBox
            options={featureOptions}
            selectedOptions={selectedFeatures}
            onChange={setSelectedFeatures}
            onCreateOption={(val) =>
              setSelectedFeatures([...selectedFeatures, { label: val }])
            }
            placeholder="Search or type features..."
            isClearable
            fullWidth
          />
        </EuiFormRow>
      )}

      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />

      {/* Step 2: Entity Source */}
      <EuiTitle size="xxs">
        <h4>Step 2: Entity Source</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiRadioGroup
        options={ENTITY_SOURCE_OPTIONS}
        idSelected={entitySourceType}
        onChange={(id) => setEntitySourceType(id)}
      />
      <EuiSpacer size="m" />

      {entitySourceType === "inline" && (
        <>
          <EuiFormRow
            label="Entity Keys (Join Keys)"
            helpText="Select which entity keys to include. These are the columns used to join features."
          >
            <EuiComboBox
              options={joinKeyOptions}
              selectedOptions={entityKeys}
              onChange={setEntityKeys}
              onCreateOption={(val) =>
                setEntityKeys([...entityKeys, { label: val }])
              }
              placeholder="Select or type entity keys (e.g. driver_id, customer_id)..."
              isClearable
              fullWidth
            />
          </EuiFormRow>

          <EuiFormRow
            label="Entity Values"
            helpText="Comma-separated values for the entity key(s). For multiple keys, use one row per entity (CSV format: key1_val,key2_val)."
          >
            <EuiTextArea
              value={entityValues}
              onChange={(e) => setEntityValues(e.target.value)}
              placeholder={
                "1001, 1002, 1003, 1004\nor for multiple keys:\n1001,A\n1002,B\n1003,C"
              }
              rows={4}
              fullWidth
            />
          </EuiFormRow>

          <EuiFormRow
            label="Time Range (optional)"
            helpText="Timestamps for point-in-time feature lookup. If omitted, current time is used for all entities."
          >
            <EuiDatePickerRange
              startDateControl={
                <EuiDatePicker
                  selected={startDate}
                  onChange={setStartDate}
                  startDate={startDate}
                  endDate={endDate}
                  showTimeSelect
                  placeholder="Start date"
                />
              }
              endDateControl={
                <EuiDatePicker
                  selected={endDate}
                  onChange={setEndDate}
                  startDate={startDate}
                  endDate={endDate}
                  showTimeSelect
                  placeholder="End date"
                />
              }
              fullWidth
            />
          </EuiFormRow>
        </>
      )}

      {entitySourceType === "reference" && (
        <EuiFormRow
          label="Data Source Path"
          helpText="Select from configured data sources or type a custom path. Must contain entity keys and event_timestamp."
        >
          <EuiComboBox
            singleSelection={{ asPlainText: true }}
            options={dataSourcePathOptions}
            selectedOptions={
              entitySourcePath ? [{ label: entitySourcePath }] : []
            }
            onChange={(selected) =>
              setEntitySourcePath(selected.length > 0 ? selected[0].label : "")
            }
            onCreateOption={(val) => setEntitySourcePath(val)}
            placeholder="Select a data source or type a path..."
            isClearable
            fullWidth
          />
        </EuiFormRow>
      )}

      <EuiSpacer size="m" />
      <EuiFormRow
        label="Additional Input Columns (optional)"
        helpText="Some on-demand features require extra input values (e.g. request-time data). Format: column_name=value (one per line). Applied to all entity rows."
      >
        <EuiTextArea
          value={extraColumns}
          onChange={(e) => setExtraColumns(e.target.value)}
          placeholder={"val_to_add=10\ndiscount_pct=0.15"}
          rows={2}
          fullWidth
        />
      </EuiFormRow>

      <EuiSpacer size="m" />
      <EuiHorizontalRule margin="s" />

      {/* Step 3: Storage Destination & Metadata */}
      <EuiTitle size="xxs">
        <h4>Step 3: Output Destination</h4>
      </EuiTitle>
      <EuiSpacer size="s" />

      <EuiFormRow
        label="Dataset Name"
        helpText="Unique name for this dataset in the catalog."
        isInvalid={
          datasetName.length > 0 &&
          !/^[a-zA-Z_][a-zA-Z0-9_-]*$/.test(datasetName)
        }
        error="Must start with letter/underscore, contain only letters, numbers, underscores, hyphens."
      >
        <EuiFieldText
          value={datasetName}
          onChange={(e) => setDatasetName(e.target.value)}
          placeholder="e.g. driver_training_2024_q1"
          fullWidth
        />
      </EuiFormRow>

      <EuiFormRow
        label="Description (optional)"
        helpText="Brief description of this dataset."
      >
        <EuiFieldText
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          placeholder="e.g. Training data for driver fraud model"
          fullWidth
        />
      </EuiFormRow>

      <EuiFlexGroup gutterSize="m">
        <EuiFlexItem>
          <EuiFormRow
            label="Namespace (optional)"
            helpText="Top-level grouping (e.g. fraud, underwriting)."
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
            label="Collection (optional)"
            helpText="Sub-grouping within namespace (e.g. raw, curated)."
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

      <EuiFlexGroup gutterSize="m">
        <EuiFlexItem grow={1}>
          <EuiFormRow label="Storage Type">
            <EuiSuperSelect
              options={storageOptions}
              valueOfSelected={storageType}
              onChange={setStorageType}
              fullWidth
            />
          </EuiFormRow>
        </EuiFlexItem>
        <EuiFlexItem grow={2}>
          <EuiFormRow
            label="Output Path"
            helpText="Where to write the generated dataset."
          >
            <EuiFieldText
              value={storagePath}
              onChange={(e) => setStoragePath(e.target.value)}
              placeholder={currentStorageType.placeholder}
              fullWidth
            />
          </EuiFormRow>
        </EuiFlexItem>
      </EuiFlexGroup>

      {storageType === "spark" && (
        <>
          <EuiSpacer size="s" />
          <EuiFormRow
            label="File Format"
            helpText="Required for Spark path-based storage. Specifies the output file format."
          >
            <EuiSuperSelect
              options={[
                {
                  value: "parquet",
                  inputDisplay: "Parquet",
                  dropdownDisplay: <strong>Parquet</strong>,
                },
                {
                  value: "avro",
                  inputDisplay: "Avro",
                  dropdownDisplay: <strong>Avro</strong>,
                },
                {
                  value: "csv",
                  inputDisplay: "CSV",
                  dropdownDisplay: <strong>CSV</strong>,
                },
                {
                  value: "json",
                  inputDisplay: "JSON",
                  dropdownDisplay: <strong>JSON</strong>,
                },
              ]}
              valueOfSelected={storageFileFormat}
              onChange={setStorageFileFormat}
              fullWidth
            />
          </EuiFormRow>
        </>
      )}

      <EuiSpacer size="m" />
      <TagsEditor tags={tags} onChange={setTags} />

      {/* Actions */}
      <EuiSpacer size="l" />
      <EuiFlexGroup justifyContent="flexEnd" gutterSize="s">
        <EuiFlexItem grow={false}>
          <EuiButtonEmpty onClick={onClose}>Cancel</EuiButtonEmpty>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiButton
            fill
            onClick={() => createMutation.mutate()}
            isLoading={createMutation.isLoading}
            disabled={!canProceedStep0 || !canProceedStep1 || !canSubmit}
            iconType="playFilled"
          >
            Create Dataset
          </EuiButton>
        </EuiFlexItem>
      </EuiFlexGroup>
    </div>
  );
};

export default CreateDatasetForm;
