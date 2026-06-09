import React, { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import {
  EuiPanel,
  EuiTitle,
  EuiForm,
  EuiFormRow,
  EuiFieldText,
  EuiButton,
  EuiSpacer,
  EuiCallOut,
  EuiText,
  EuiLoadingSpinner,
  EuiFlexGroup,
  EuiFlexItem,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiBadge,
  EuiDatePicker,
  EuiSuperSelect,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadLabelView from "./useLoadLabelView";
import useLoadRegistry from "../../queries/useLoadRegistry";
import moment from "moment";

const TrainingExportTab = () => {
  const { labelViewName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const name = labelViewName || "";
  const { isLoading, isSuccess, data } = useLoadLabelView(name);
  const { data: registryData } = useLoadRegistry(registryUrl);

  const [featureService, setFeatureService] = useState("");
  const [entityColumn, setEntityColumn] = useState("");
  const [entityValues, setEntityValues] = useState("");
  const [startDate, setStartDate] = useState<moment.Moment | null>(
    moment().subtract(30, "days"),
  );
  const [endDate, setEndDate] = useState<moment.Moment | null>(moment());
  const [exportFormat, setExportFormat] = useState("csv");
  const [exporting, setExporting] = useState(false);
  const [exportResult, setExportResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  if (isLoading) {
    return (
      <p>
        <EuiLoadingSpinner size="m" /> Loading...
      </p>
    );
  }

  const spec = data?.object?.spec || data?.spec || {};
  const entities: string[] = spec.entities || [];

  const allFeatureServices = registryData?.objects?.featureServices || [];
  const relevantFeatureServices = allFeatureServices.filter((fs: any) => {
    const projections = [
      ...(fs.spec?.features || []),
      ...(fs.spec?.featureViewProjections || []),
    ];
    return projections.some(
      (proj: any) =>
        proj.featureViewName === name ||
        proj.name === name ||
        proj.featureViewProjection?.featureViewName === name,
    );
  });
  const servicesToShow =
    relevantFeatureServices.length > 0
      ? relevantFeatureServices
      : allFeatureServices;

  const featureServiceOptions = servicesToShow.map((fs: any) => ({
    value: fs.spec?.name || fs.name || "",
    inputDisplay: fs.spec?.name || fs.name || "Unknown",
    dropdownDisplay: (
      <span>
        <strong>{fs.spec?.name || fs.name}</strong>
        {fs.spec?.description && (
          <EuiText size="xs" color="subdued">
            <p style={{ margin: 0 }}>{fs.spec.description}</p>
          </EuiText>
        )}
      </span>
    ),
  }));

  const entityColumnOptions = entities.map((e: string) => ({
    value: e,
    inputDisplay: e,
  }));

  const handleExport = async () => {
    setExporting(true);
    setError(null);
    setExportResult(null);

    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const entityKey =
        entityColumn || (entities.length > 0 ? entities[0] : "entity_id");
      const values = entityValues
        .split(",")
        .map((v: string) => v.trim())
        .filter((v: string) => v.length > 0);

      if (values.length === 0) {
        setError("Please provide at least one entity value");
        setExporting(false);
        return;
      }

      const serviceName = featureService || `${name}_service`;
      const entityDf: Record<string, any[]> = {
        [entityKey]: values,
      };

      const response = await fetch(`${baseUrl}/training-dataset/export`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          feature_service: serviceName,
          entity_df: entityDf,
          start_date: startDate?.toISOString() || null,
          end_date: endDate?.toISOString() || null,
        }),
      });

      const result = await response.json();
      if (!response.ok) {
        const detail = result.detail;
        setError(
          typeof detail === "string"
            ? detail
            : Array.isArray(detail)
              ? detail.map((d: any) => d.msg || JSON.stringify(d)).join("; ")
              : "Export failed",
        );
      } else {
        setExportResult(result);
      }
    } catch (e: any) {
      setError(e.message || "Network error");
    } finally {
      setExporting(false);
    }
  };

  const downloadCSV = () => {
    if (!exportResult?.data) return;
    const cols = exportResult.columns;
    const csvRows = [cols.join(",")];
    for (const row of exportResult.data) {
      csvRows.push(
        cols
          .map((c: string) => {
            const val = row[c];
            if (val === null || val === undefined) return "";
            const str = String(val);
            return str.includes(",") ? `"${str}"` : str;
          })
          .join(","),
      );
    }
    const blob = new Blob([csvRows.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${exportResult.feature_service}_training_data.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const downloadJSON = () => {
    if (!exportResult?.data) return;
    const blob = new Blob([JSON.stringify(exportResult.data, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${exportResult.feature_service}_training_data.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const columns: EuiBasicTableColumn<any>[] = exportResult
    ? exportResult.columns.map((col: string) => ({
        field: col,
        name: col,
        truncateText: true,
        render: (val: any) =>
          val === null || val === undefined ? (
            <EuiBadge color="hollow">null</EuiBadge>
          ) : (
            String(val)
          ),
      }))
    : [];

  return (
    <React.Fragment>
      <EuiCallOut
        title="Generate Training Dataset"
        color="primary"
        iconType="exportAction"
      >
        <EuiText size="s">
          Create a point-in-time correct training dataset by joining features
          with labels via <code>get_historical_features</code>. Export as CSV or
          JSON for model training.
        </EuiText>
      </EuiCallOut>

      <EuiSpacer size="l" />

      <EuiPanel>
        <EuiTitle size="xs">
          <h3>Export Configuration</h3>
        </EuiTitle>
        <EuiSpacer size="m" />

        <EuiForm>
          <EuiFormRow
            label="Feature Service"
            helpText="A Feature Service bundles multiple Feature Views (features + labels) into a single training dataset via point-in-time joins"
          >
            <EuiSuperSelect
              options={featureServiceOptions}
              valueOfSelected={featureService}
              onChange={(value) => setFeatureService(value)}
              placeholder="Select a feature service..."
              hasDividers
            />
          </EuiFormRow>

          <EuiFormRow
            label="Entity Column"
            helpText="The entity join key for the training dataset"
          >
            {entityColumnOptions.length > 0 ? (
              <EuiSuperSelect
                options={entityColumnOptions}
                valueOfSelected={entityColumn || entities[0] || ""}
                onChange={(value) => setEntityColumn(value)}
              />
            ) : (
              <EuiFieldText
                placeholder="entity_id"
                value={entityColumn}
                onChange={(e) => setEntityColumn(e.target.value)}
              />
            )}
          </EuiFormRow>

          <EuiFormRow
            label="Entity Values"
            helpText="Comma-separated entity IDs to include in training set"
          >
            <EuiFieldText
              placeholder="user_1, user_2, user_3"
              value={entityValues}
              onChange={(e) => setEntityValues(e.target.value)}
            />
          </EuiFormRow>

          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiFormRow label="Start Date">
                <EuiDatePicker
                  selected={startDate}
                  onChange={setStartDate}
                  dateFormat="YYYY-MM-DD"
                />
              </EuiFormRow>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiFormRow label="End Date">
                <EuiDatePicker
                  selected={endDate}
                  onChange={setEndDate}
                  dateFormat="YYYY-MM-DD"
                />
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>

          <EuiSpacer size="m" />

          <EuiButton
            fill
            onClick={handleExport}
            isLoading={exporting}
            iconType="exportAction"
          >
            Generate Training Dataset
          </EuiButton>
        </EuiForm>
      </EuiPanel>

      {error && (
        <React.Fragment>
          <EuiSpacer size="m" />
          <EuiCallOut title="Export Error" color="danger" iconType="alert">
            {error}
          </EuiCallOut>
        </React.Fragment>
      )}

      {exportResult && (
        <React.Fragment>
          <EuiSpacer size="l" />
          <EuiPanel>
            <EuiFlexGroup alignItems="center">
              <EuiFlexItem>
                <EuiTitle size="xs">
                  <h3>
                    Training Dataset{" "}
                    <EuiBadge color="success">
                      {exportResult.row_count} rows
                    </EuiBadge>
                  </h3>
                </EuiTitle>
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiFlexGroup gutterSize="s">
                  <EuiFlexItem>
                    <EuiButton
                      size="s"
                      onClick={downloadCSV}
                      iconType="exportAction"
                    >
                      Download CSV
                    </EuiButton>
                  </EuiFlexItem>
                  <EuiFlexItem>
                    <EuiButton
                      size="s"
                      onClick={downloadJSON}
                      iconType="document"
                    >
                      Download JSON
                    </EuiButton>
                  </EuiFlexItem>
                </EuiFlexGroup>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
            <EuiText size="xs" color="subdued">
              Feature service: <strong>{exportResult.feature_service}</strong> |
              Columns: {exportResult.columns.length} | Point-in-time correct
            </EuiText>
            <EuiSpacer size="m" />
            <EuiBasicTable
              items={exportResult.data.slice(0, 50)}
              columns={columns}
              tableLayout="auto"
            />
            {exportResult.data.length > 50 && (
              <EuiText size="xs" color="subdued">
                Showing first 50 of {exportResult.data.length} rows. Download
                for full dataset.
              </EuiText>
            )}
          </EuiPanel>
        </React.Fragment>
      )}

      <EuiSpacer size="l" />

      <EuiPanel color="subdued">
        <EuiTitle size="xxs">
          <h4>SDK Equivalent</h4>
        </EuiTitle>
        <EuiSpacer size="s" />
        <EuiText size="xs" color="subdued">
          This UI action is equivalent to the following Python SDK call:
        </EuiText>
        <EuiSpacer size="s" />
        <pre
          style={{
            fontSize: "12px",
            padding: "8px",
            background: "#1a1a2e",
            color: "#e0e0e0",
            borderRadius: "4px",
            overflow: "auto",
          }}
        >
          {`from feast import FeatureStore
import pandas as pd

store = FeatureStore(".")
entity_df = pd.DataFrame({
    "${entityColumn || entities[0] || "entity_id"}": [${
      entityValues
        ? entityValues
            .split(",")
            .map((v) => `"${v.trim()}"`)
            .join(", ")
        : '"user_1", "user_2"'
    }],
    "event_timestamp": pd.Timestamp("${endDate?.toISOString() || "now"}"),
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=store.get_feature_service("${featureService || "your_service"}"),
).to_df()

training_df.to_parquet("training_data.parquet")`}
        </pre>
      </EuiPanel>
    </React.Fragment>
  );
};

export default TrainingExportTab;
