import React, { useState, useContext, useCallback } from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiLoadingSpinner,
  EuiText,
  EuiCallOut,
  EuiButton,
  EuiSpacer,
  EuiPanel,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiFieldNumber,
  EuiFormRow,
  EuiBadge,
  EuiTitle,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { useDataMode } from "../../contexts/DataModeContext";

const DatasetSampleTab = () => {
  const { datasetName, projectName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sampleData, setSampleData] = useState<{
    columns: string[];
    rows: Record<string, any>[];
    total_rows: number;
    sample_size: number;
  } | null>(null);
  const [limit, setLimit] = useState(10);

  const fetchSample = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(
        `${registryUrl}/saved_datasets/data/${encodeURIComponent(datasetName || "")}?project=${encodeURIComponent(projectName || "")}&limit=${limit}`,
        { method: "GET", ...fetchOptions },
      );

      if (!response.ok) {
        const err = await response
          .json()
          .catch(() => ({ detail: "Failed to load sample" }));
        throw new Error(err.detail || `Request failed: ${response.status}`);
      }

      const data = await response.json();
      setSampleData(data);
    } catch (err: any) {
      setError(err.message || "Failed to load dataset sample.");
    } finally {
      setLoading(false);
    }
  }, [registryUrl, datasetName, projectName, limit, fetchOptions]);

  const columns: EuiBasicTableColumn<Record<string, any>>[] = sampleData
    ? sampleData.columns.map((col) => ({
        field: col,
        name: col,
        sortable: true,
        truncateText: true,
        render: (value: any) => {
          if (value === null || value === undefined || value === "") {
            return (
              <EuiText size="xs" color="subdued">
                null
              </EuiText>
            );
          }
          return String(value);
        },
      }))
    : [];

  return (
    <>
      <EuiPanel hasBorder>
        <EuiFlexGroup alignItems="flexEnd" gutterSize="m">
          <EuiFlexItem grow={false}>
            <EuiFormRow label="Rows to preview" display="columnCompressed">
              <EuiFieldNumber
                value={limit}
                onChange={(e) => setLimit(parseInt(e.target.value) || 10)}
                min={1}
                max={100}
                compressed
                style={{ width: 80 }}
              />
            </EuiFormRow>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButton
              onClick={fetchSample}
              isLoading={loading}
              iconType="inspect"
              size="s"
            >
              Load Preview
            </EuiButton>
          </EuiFlexItem>
          {sampleData && (
            <EuiFlexItem grow={false}>
              <EuiBadge color="hollow">
                {sampleData.sample_size} of {sampleData.total_rows} rows
              </EuiBadge>
            </EuiFlexItem>
          )}
        </EuiFlexGroup>
      </EuiPanel>

      <EuiSpacer size="m" />

      {error && (
        <>
          <EuiCallOut
            title="Failed to load preview"
            color="danger"
            iconType="alert"
            size="s"
          >
            <p>{error}</p>
          </EuiCallOut>
          <EuiSpacer size="m" />
        </>
      )}

      {loading && (
        <EuiFlexGroup justifyContent="center" alignItems="center">
          <EuiFlexItem grow={false}>
            <EuiLoadingSpinner size="l" />
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiText>Reading dataset...</EuiText>
          </EuiFlexItem>
        </EuiFlexGroup>
      )}

      {!loading && !sampleData && !error && (
        <EuiPanel color="subdued" paddingSize="l" hasBorder={false}>
          <EuiFlexGroup
            justifyContent="center"
            alignItems="center"
            direction="column"
          >
            <EuiFlexItem>
              <EuiTitle size="xs">
                <h3>Preview dataset contents</h3>
              </EuiTitle>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiText size="s" color="subdued" textAlign="center">
                Click "Load Preview" to read and display actual rows from the
                dataset's storage location. This executes a read query against
                the configured offline store.
              </EuiText>
            </EuiFlexItem>
          </EuiFlexGroup>
        </EuiPanel>
      )}

      {sampleData && sampleData.rows.length > 0 && (
        <EuiBasicTable
          items={sampleData.rows}
          columns={columns}
          tableLayout="auto"
        />
      )}

      {sampleData && sampleData.rows.length === 0 && (
        <EuiCallOut
          title="Dataset is empty"
          color="warning"
          iconType="alert"
          size="s"
        >
          <p>No rows found in this dataset's storage location.</p>
        </EuiCallOut>
      )}
    </>
  );
};

export default DatasetSampleTab;
