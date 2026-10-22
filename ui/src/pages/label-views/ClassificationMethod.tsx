import React, {
  useState,
  useContext,
  useEffect,
  useCallback,
  useMemo,
} from "react";
import { useParams } from "react-router-dom";
import {
  EuiCallOut,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiButton,
  EuiPanel,
  EuiText,
  EuiLoadingSpinner,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiSelect,
  EuiBadge,
  EuiFieldSearch,
  EuiTablePagination,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadLabelView from "./useLoadLabelView";
import useAnnotationConfig from "./useAnnotationConfig";

interface LabelRow {
  _id: string;
  [key: string]: any;
}

const PAGE_SIZE_OPTIONS = [10, 25, 50];

const ClassificationMethod = () => {
  const { labelViewName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const { data } = useLoadLabelView(labelViewName || "");
  const { data: annotationConfig } = useAnnotationConfig(labelViewName || "");

  const [rows, setRows] = useState<LabelRow[]>([]);
  const [originalRows, setOriginalRows] = useState<LabelRow[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [pushSuccess, setPushSuccess] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(25);

  const spec = data?.object?.spec || data?.spec || {};
  const labelFields: { name: string; valueType?: string }[] = useMemo(
    () => spec.features || [],
    [spec.features],
  );
  const entities: string[] = useMemo(
    () =>
      spec.entityColumns?.length
        ? spec.entityColumns.map((ec: { name: string }) => ec.name)
        : spec.entities || [],
    [spec.entityColumns, spec.entities],
  );

  const configuredValues = annotationConfig?.label_values || {};
  const fieldRoles = annotationConfig?.field_roles || {};
  const labelWidgets = annotationConfig?.label_widgets || {};

  const fetchLabels = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const response = await fetch(`${baseUrl}/list-labels`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          feature_view: labelViewName || "",
          limit: 200,
        }),
      });
      if (!response.ok) {
        throw new Error(`Failed to fetch labels (${response.status})`);
      }
      const result = await response.json();
      const labelData: Record<string, any>[] = result.labels || [];
      const mapped = labelData.map((row, idx) => ({
        ...row,
        _id: `row_${idx}_${Object.values(row).join("_")}`,
      }));
      setRows(mapped);
      setOriginalRows(JSON.parse(JSON.stringify(mapped)));
    } catch (e: any) {
      setError(e.message || "Failed to load labels");
    } finally {
      setIsLoading(false);
    }
  }, [labelViewName, registryUrl]);

  useEffect(() => {
    if (labelViewName) {
      fetchLabels();
    }
  }, [labelViewName, fetchLabels]);

  const handleFieldChange = (rowId: string, field: string, value: string) => {
    setRows((prev) =>
      prev.map((row) => (row._id === rowId ? { ...row, [field]: value } : row)),
    );
  };

  const getChangedRows = () => {
    return rows.filter((row) => {
      const original = originalRows.find((o) => o._id === row._id);
      if (!original) return false;
      return labelFields.some((f) => row[f.name] !== original[f.name]);
    });
  };

  const resetChanges = () => {
    setRows(JSON.parse(JSON.stringify(originalRows)));
  };

  const saveToLabelView = async () => {
    const changed = getChangedRows();
    if (changed.length === 0) return;

    setIsSaving(true);
    setError(null);
    setPushSuccess(null);

    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const pushSourceName =
        spec.source?.pushSourceName ||
        spec.source?.name ||
        `${labelViewName}_push_source`;

      const pushRows = changed.map((row) => {
        const pushRow: Record<string, any> = {};
        entities.forEach((e) => {
          pushRow[e] = row[e];
        });
        labelFields.forEach((f) => {
          pushRow[f.name] = row[f.name];
        });
        pushRow["event_timestamp"] = new Date().toISOString();
        return pushRow;
      });

      const columnar: Record<string, any[]> = {};
      if (pushRows.length > 0) {
        for (const key of Object.keys(pushRows[0])) {
          columnar[key] = pushRows.map((r) => r[key]);
        }
      }

      const response = await fetch(`${baseUrl}/push`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          push_source_name: pushSourceName,
          df: columnar,
          to: "online_and_offline",
        }),
      });

      if (response.ok) {
        setPushSuccess(
          `Successfully pushed ${changed.length} updated labels to ${labelViewName}`,
        );
        setOriginalRows(JSON.parse(JSON.stringify(rows)));
      } else {
        const errData = await response.json().catch(() => null);
        setError(errData?.detail || `Push failed (${response.status})`);
      }
    } catch (e: any) {
      setError(e.message || "Network error");
    } finally {
      setIsSaving(false);
    }
  };

  const exportJSON = () => {
    const exportData = rows.map((row) => {
      const { _id, ...rest } = row;
      return rest;
    });
    const blob = new Blob([JSON.stringify(exportData, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${labelViewName}_labels.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const filteredRows = useMemo(() => {
    if (!searchQuery.trim()) return rows;
    const q = searchQuery.toLowerCase();
    return rows.filter((row) =>
      Object.entries(row)
        .filter(([key]) => key !== "_id")
        .some(
          ([, val]) => val != null && String(val).toLowerCase().includes(q),
        ),
    );
  }, [rows, searchQuery]);

  const paginatedRows = useMemo(() => {
    const start = pageIndex * pageSize;
    return filteredRows.slice(start, start + pageSize);
  }, [filteredRows, pageIndex, pageSize]);

  const uniqueValuesForField = useMemo(() => {
    const result: Record<string, string[]> = {};
    labelFields.forEach((field) => {
      const values = new Set<string>();
      rows.forEach((row) => {
        if (row[field.name] != null && String(row[field.name]).trim() !== "") {
          values.add(String(row[field.name]));
        }
      });
      result[field.name] = Array.from(values).sort();
    });
    return result;
  }, [rows, labelFields]);

  const metadataFields = ["event_timestamp", "labeler"];

  const entityColumns: EuiBasicTableColumn<LabelRow>[] = entities.map(
    (ent) => ({
      field: ent,
      name: ent,
      sortable: true,
      truncateText: true,
    }),
  );

  const labelColumns: EuiBasicTableColumn<LabelRow>[] = labelFields
    .filter((field) => !metadataFields.includes(field.name))
    .map((field) => {
      const configVals = configuredValues[field.name];
      const role = fieldRoles[field.name];
      const widget = labelWidgets[field.name];

      return {
        field: field.name,
        name: field.name,
        render: (value: any, row: LabelRow) => {
          if (widget === "binary" && configVals && configVals.length === 2) {
            return (
              <EuiSelect
                compressed
                options={[
                  { value: "", text: "— select —" },
                  ...configVals.map((o: string) => ({
                    value: o,
                    text: o === "1" ? "Yes (1)" : o === "0" ? "No (0)" : o,
                  })),
                ]}
                value={value != null ? String(value) : ""}
                onChange={(e) =>
                  handleFieldChange(row._id, field.name, e.target.value)
                }
              />
            );
          }
          const dropdownOptions =
            configVals || uniqueValuesForField[field.name] || [];
          if (
            (role === "label" || dropdownOptions.length > 0) &&
            dropdownOptions.length <= 30
          ) {
            return (
              <EuiSelect
                compressed
                options={[
                  { value: "", text: "— select —" },
                  ...dropdownOptions.map((o: string) => ({
                    value: o,
                    text: o,
                  })),
                ]}
                value={value != null ? String(value) : ""}
                onChange={(e) =>
                  handleFieldChange(row._id, field.name, e.target.value)
                }
              />
            );
          }
          return <span>{value != null ? String(value) : ""}</span>;
        },
      };
    });

  const metadataColumns: EuiBasicTableColumn<LabelRow>[] = labelFields
    .filter((field) => metadataFields.includes(field.name))
    .map((field) => ({
      field: field.name,
      name: field.name,
      sortable: true,
      truncateText: true,
    }));

  const columns = [...entityColumns, ...labelColumns, ...metadataColumns];

  const changedCount = getChangedRows().length;

  if (isLoading) {
    return (
      <EuiFlexGroup alignItems="center" gutterSize="s">
        <EuiFlexItem grow={false}>
          <EuiLoadingSpinner size="m" />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiText>Loading labels from {labelViewName}...</EuiText>
        </EuiFlexItem>
      </EuiFlexGroup>
    );
  }

  return (
    <React.Fragment>
      <EuiCallOut
        title="Review & Edit labels"
        color="primary"
        iconType="tableDensityNormal"
        size="s"
      >
        <p>
          Review and correct existing labels in the table below. Changes are
          pushed to <strong>{labelViewName}</strong> via its PushSource and
          governed by the configured conflict policy.
        </p>
      </EuiCallOut>

      {error && (
        <>
          <EuiSpacer size="m" />
          <EuiCallOut title="Error" color="danger" iconType="alert">
            <p>{error}</p>
          </EuiCallOut>
        </>
      )}

      {pushSuccess && (
        <>
          <EuiSpacer size="m" />
          <EuiCallOut title={pushSuccess} color="success" iconType="check" />
        </>
      )}

      <EuiSpacer size="l" />

      {rows.length === 0 ? (
        <EuiPanel>
          <EuiText textAlign="center" color="subdued">
            <p>
              No labels found. Use Entity Form or Active Learning to add labels
              first, then review them here.
            </p>
          </EuiText>
        </EuiPanel>
      ) : (
        <>
          <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
            <EuiFlexItem>
              <EuiFieldSearch
                placeholder="Search labels..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setPageIndex(0);
                }}
                isClearable
              />
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiFlexGroup gutterSize="s">
                {changedCount > 0 && (
                  <EuiFlexItem grow={false}>
                    <EuiBadge color="warning">{changedCount} changed</EuiBadge>
                  </EuiFlexItem>
                )}
                <EuiFlexItem grow={false}>
                  <EuiButton
                    size="s"
                    onClick={exportJSON}
                    iconType="exportAction"
                  >
                    Export JSON
                  </EuiButton>
                </EuiFlexItem>
                <EuiFlexItem grow={false}>
                  <EuiButton
                    size="s"
                    color="warning"
                    onClick={resetChanges}
                    disabled={changedCount === 0}
                  >
                    Reset
                  </EuiButton>
                </EuiFlexItem>
                <EuiFlexItem grow={false}>
                  <EuiButton
                    size="s"
                    fill
                    color="success"
                    onClick={saveToLabelView}
                    isLoading={isSaving}
                    disabled={changedCount === 0}
                    iconType="push"
                  >
                    Save ({changedCount})
                  </EuiButton>
                </EuiFlexItem>
              </EuiFlexGroup>
            </EuiFlexItem>
          </EuiFlexGroup>

          <EuiSpacer size="m" />

          <EuiPanel paddingSize="none">
            <EuiBasicTable
              items={paginatedRows}
              columns={columns}
              tableLayout="auto"
              rowProps={(row: LabelRow) => {
                const original = originalRows.find((o) => o._id === row._id);
                const isChanged =
                  original &&
                  labelFields.some((f) => row[f.name] !== original[f.name]);
                return isChanged
                  ? { style: { backgroundColor: "rgba(255, 200, 0, 0.05)" } }
                  : {};
              }}
            />
          </EuiPanel>

          {filteredRows.length > pageSize && (
            <>
              <EuiSpacer size="m" />
              <EuiTablePagination
                pageCount={Math.ceil(filteredRows.length / pageSize)}
                activePage={pageIndex}
                onChangePage={(page) => setPageIndex(page)}
                itemsPerPage={pageSize}
                onChangeItemsPerPage={(size) => {
                  setPageSize(size);
                  setPageIndex(0);
                }}
                itemsPerPageOptions={PAGE_SIZE_OPTIONS}
              />
            </>
          )}
        </>
      )}
    </React.Fragment>
  );
};

export default ClassificationMethod;
