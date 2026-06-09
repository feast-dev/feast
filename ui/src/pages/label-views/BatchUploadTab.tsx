import React, { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import {
  EuiPanel,
  EuiTitle,
  EuiHorizontalRule,
  EuiForm,
  EuiFormRow,
  EuiButton,
  EuiSpacer,
  EuiCallOut,
  EuiText,
  EuiLoadingSpinner,
  EuiFlexGroup,
  EuiFlexItem,
  EuiBadge,
  EuiSelect,
  EuiFilePicker,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiIcon,
  EuiCodeBlock,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadLabelView from "./useLoadLabelView";

const BatchUploadTab = () => {
  const { labelViewName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const name = labelViewName || "";
  const { isLoading, isSuccess, data } = useLoadLabelView(name);

  const [fileData, setFileData] = useState<any[] | null>(null);
  const [fileName, setFileName] = useState<string>("");
  const [pushTarget, setPushTarget] = useState("online");
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);

  if (isLoading) {
    return (
      <p>
        <EuiLoadingSpinner size="m" /> Loading...
      </p>
    );
  }

  const spec = data?.object?.spec || data?.spec || {};
  const pushSourceName =
    spec.streamSource?.pushOptions?.pushSourceName ||
    spec.batchSource?.name?.replace("_batch", "_push_source") ||
    `${name}_push_source`;

  const features = data?.features || spec.features || [];
  const entities = spec.entityColumns?.length
    ? spec.entityColumns.map((ec: { name: string }) => ec.name)
    : spec.entities || [];

  const handleFileChange = (files: FileList | null) => {
    if (!files || files.length === 0) {
      setFileData(null);
      setFileName("");
      setParseError(null);
      return;
    }

    const file = files[0];
    setFileName(file.name);
    setParseError(null);

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const text = e.target?.result as string;
        if (file.name.endsWith(".json")) {
          const parsed = JSON.parse(text);
          const rows = Array.isArray(parsed)
            ? parsed
            : parsed.data || parsed.records || [parsed];
          setFileData(rows);
        } else if (file.name.endsWith(".csv")) {
          const lines = text.trim().split("\n");
          if (lines.length < 2) {
            setParseError(
              "CSV must have at least a header row and one data row",
            );
            return;
          }
          const headers = lines[0]
            .split(",")
            .map((h) => h.trim().replace(/"/g, ""));
          const rows = [];
          for (let i = 1; i < lines.length; i++) {
            const values = lines[i]
              .split(",")
              .map((v) => v.trim().replace(/"/g, ""));
            const row: Record<string, any> = {};
            headers.forEach((h, idx) => {
              const val = values[idx] || "";
              const numVal = Number(val);
              row[h] = val === "" ? null : isNaN(numVal) ? val : numVal;
            });
            rows.push(row);
          }
          setFileData(rows);
        } else {
          setParseError("Unsupported file format. Use .csv or .json");
        }
      } catch (e: any) {
        setParseError(`Failed to parse file: ${e.message}`);
        setFileData(null);
      }
    };
    reader.readAsText(file);
  };

  const handleUpload = async () => {
    if (!fileData || fileData.length === 0) return;

    setUploading(true);
    setError(null);
    setResult(null);

    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";

      const dataWithTimestamp = fileData.map((row) => ({
        ...row,
        event_timestamp: row.event_timestamp || new Date().toISOString(),
      }));

      const response = await fetch(`${baseUrl}/batch-push`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          push_source_name: pushSourceName,
          data: dataWithTimestamp,
          to: pushTarget,
        }),
      });

      const res = await response.json();
      if (!response.ok) {
        const detail = res.detail;
        setError(
          typeof detail === "string"
            ? detail
            : Array.isArray(detail)
              ? detail.map((d: any) => d.msg || JSON.stringify(d)).join("; ")
              : "Upload failed",
        );
      } else {
        setResult(res);
      }
    } catch (e: any) {
      setError(e.message || "Network error");
    } finally {
      setUploading(false);
    }
  };

  const previewColumns: EuiBasicTableColumn<any>[] = fileData
    ? Object.keys(fileData[0] || {}).map((col) => ({
        field: col,
        name: col,
        truncateText: true,
        width: "120px",
      }))
    : [];

  const csvTemplate = [
    entities.join(",") +
      "," +
      features.map((f: any) => f.name || f).join(",") +
      ",event_timestamp",
    entities.map(() => "<value>").join(",") +
      "," +
      features.map(() => "<value>").join(",") +
      ",2026-01-01T00:00:00Z",
  ].join("\n");

  return (
    <React.Fragment>
      <EuiCallOut
        title="Batch Label Upload"
        color="primary"
        iconType="importAction"
      >
        <EuiText size="s">
          Upload a CSV or JSON file to push labels in bulk. Useful for
          correcting labels, importing from external systems, or backfilling
          historical labels.
        </EuiText>
      </EuiCallOut>

      <EuiSpacer size="l" />

      <EuiPanel>
        <EuiTitle size="xs">
          <h3>Upload File</h3>
        </EuiTitle>
        <EuiSpacer size="m" />

        <EuiForm>
          <EuiFormRow
            label="CSV or JSON file"
            helpText="CSV: header row + data rows. JSON: array of objects."
          >
            <EuiFilePicker
              accept=".csv,.json"
              onChange={handleFileChange}
              display="large"
              initialPromptText="Drop a .csv or .json file here"
            />
          </EuiFormRow>

          {parseError && (
            <React.Fragment>
              <EuiSpacer size="s" />
              <EuiCallOut
                title="Parse Error"
                color="danger"
                iconType="alert"
                size="s"
              >
                {parseError}
              </EuiCallOut>
            </React.Fragment>
          )}

          <EuiFormRow label="Push target">
            <EuiSelect
              options={[
                { value: "online", text: "Online store only" },
                { value: "online_and_offline", text: "Online + Offline" },
                { value: "offline", text: "Offline store only" },
              ]}
              value={pushTarget}
              onChange={(e) => setPushTarget(e.target.value)}
            />
          </EuiFormRow>

          <EuiSpacer size="m" />

          <EuiButton
            fill
            onClick={handleUpload}
            isLoading={uploading}
            disabled={!fileData || fileData.length === 0}
            iconType="importAction"
          >
            Push {fileData ? `${fileData.length} rows` : "Labels"}
          </EuiButton>
        </EuiForm>
      </EuiPanel>

      {/* Preview */}
      {fileData && fileData.length > 0 && (
        <React.Fragment>
          <EuiSpacer size="m" />
          <EuiPanel>
            <EuiFlexGroup alignItems="center">
              <EuiFlexItem>
                <EuiTitle size="xxs">
                  <h4>
                    Preview{" "}
                    <EuiBadge color="primary">{fileData.length} rows</EuiBadge>{" "}
                    <EuiBadge color="hollow">{fileName}</EuiBadge>
                  </h4>
                </EuiTitle>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="s" />
            <EuiBasicTable
              items={fileData.slice(0, 10)}
              columns={previewColumns}
              tableLayout="auto"
            />
            {fileData.length > 10 && (
              <EuiText size="xs" color="subdued">
                Showing first 10 of {fileData.length} rows
              </EuiText>
            )}
          </EuiPanel>
        </React.Fragment>
      )}

      {error && (
        <React.Fragment>
          <EuiSpacer size="m" />
          <EuiCallOut title="Upload Error" color="danger" iconType="alert">
            {error}
          </EuiCallOut>
        </React.Fragment>
      )}

      {result && (
        <React.Fragment>
          <EuiSpacer size="m" />
          <EuiCallOut
            title="Upload Successful"
            color="success"
            iconType="check"
          >
            <EuiText size="s">
              Pushed <strong>{result.rows_pushed}</strong> rows to{" "}
              <strong>{pushTarget}</strong> store.
            </EuiText>
          </EuiCallOut>
        </React.Fragment>
      )}

      <EuiSpacer size="l" />

      {/* Template */}
      <EuiPanel color="subdued">
        <EuiTitle size="xxs">
          <h4>CSV Template</h4>
        </EuiTitle>
        <EuiSpacer size="s" />
        <EuiText size="xs" color="subdued">
          Expected columns for this LabelView:
        </EuiText>
        <EuiSpacer size="s" />
        <EuiCodeBlock language="csv" paddingSize="s" isCopyable>
          {csvTemplate}
        </EuiCodeBlock>
      </EuiPanel>
    </React.Fragment>
  );
};

export default BatchUploadTab;
