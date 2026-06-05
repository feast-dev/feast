import React, { useContext, useState, useEffect, useMemo } from "react";
import { useParams } from "react-router-dom";
import {
  EuiPanel,
  EuiTitle,
  EuiHorizontalRule,
  EuiFieldSearch,
  EuiButton,
  EuiSpacer,
  EuiCallOut,
  EuiText,
  EuiLoadingSpinner,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiBadge,
  EuiFlexGroup,
  EuiFlexItem,
  EuiTablePagination,
  EuiEmptyPrompt,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadLabelView from "./useLoadLabelView";

interface LabelRow {
  [key: string]: any;
}

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

const LabelBrowseTab = () => {
  const { labelViewName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const name = labelViewName || "";
  const { isLoading, isSuccess, data } = useLoadLabelView(name);

  const [allLabels, setAllLabels] = useState<LabelRow[] | null>(null);
  const [allEntityNames, setAllEntityNames] = useState<string[]>([]);
  const [totalEntities, setTotalEntities] = useState<number>(0);
  const [loadingAll, setLoadingAll] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(25);
  const initialLoadDone = React.useRef(false);

  useEffect(() => {
    if (isSuccess && data && !initialLoadDone.current) {
      initialLoadDone.current = true;
      loadLabels();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isSuccess, data, name, registryUrl]);

  const loadLabels = async () => {
    setLoadingAll(true);
    setError(null);
    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const response = await fetch(`${baseUrl}/list-labels`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ feature_view: name, limit: 1000 }),
      });
      if (response.ok) {
        const respData = await response.json();
        setAllLabels(respData.labels || []);
        setAllEntityNames(respData.entity_names || []);
        setTotalEntities(respData.total_entities || 0);
      } else {
        const errData = await response.json().catch(() => null);
        setError(
          errData?.detail || `Failed to load labels (${response.status})`,
        );
      }
    } catch (err: any) {
      setError(err.message || "Network error.");
    } finally {
      setLoadingAll(false);
    }
  };

  const spec = data?.spec || data?.object?.spec || {};
  const entities: string[] = spec.entities || [];
  const features: any[] = spec.features || [];
  const conflictPolicy =
    spec.conflictPolicy || spec.conflict_policy || "LAST_WRITE_WINS";
  const policyLabel =
    typeof conflictPolicy === "number"
      ? ["LAST_WRITE_WINS", "LABELER_PRIORITY", "MAJORITY_VOTE"][
          conflictPolicy
        ] || "LAST_WRITE_WINS"
      : String(conflictPolicy).replace("CONFLICT_POLICY_", "");
  const labelerField: string =
    spec.labelerField || spec.labeler_field || "labeler";

  const entityCols = allEntityNames.length > 0 ? allEntityNames : entities;

  const filteredLabels = useMemo(() => {
    if (!allLabels) return [];
    if (!searchQuery.trim()) return allLabels;

    const query = searchQuery.toLowerCase();
    return allLabels.filter((row) =>
      Object.values(row).some(
        (val) => val != null && String(val).toLowerCase().includes(query),
      ),
    );
  }, [allLabels, searchQuery]);

  const paginatedLabels = useMemo(() => {
    const start = pageIndex * pageSize;
    return filteredLabels.slice(start, start + pageSize);
  }, [filteredLabels, pageIndex, pageSize]);

  const columns: EuiBasicTableColumn<LabelRow>[] = useMemo(() => {
    const cols: EuiBasicTableColumn<LabelRow>[] = [];

    for (const entity of entityCols) {
      cols.push({
        field: entity,
        name: entity,
        sortable: true,
        render: (value: any) => (
          <EuiBadge color="hollow">
            {value != null ? String(value) : "\u2014"}
          </EuiBadge>
        ),
      });
    }

    for (const feature of features) {
      cols.push({
        field: feature.name,
        name: feature.name,
        sortable: true,
        render: (value: any) => {
          if (value === null || value === undefined) {
            return (
              <EuiText size="xs" color="subdued">
                <em>&mdash;</em>
              </EuiText>
            );
          }
          return <strong>{String(value)}</strong>;
        },
      });
    }

    cols.push({
      field: "_event_ts",
      name: "Last Updated",
      sortable: true,
      render: (value: any) =>
        value ? new Date(value * 1000).toLocaleString() : "\u2014",
    });

    return cols;
  }, [entityCols, features]);

  if (isLoading) {
    return (
      <p>
        <EuiLoadingSpinner size="m" /> Loading schema...
      </p>
    );
  }

  if (!isSuccess || !data) {
    return <p>Unable to load label view schema.</p>;
  }

  return (
    <React.Fragment>
      <EuiPanel hasBorder paddingSize="m">
        <EuiFlexGroup gutterSize="l" responsive={false}>
          <EuiFlexItem grow={2}>
            <EuiTitle size="xxs">
              <h4>Schema</h4>
            </EuiTitle>
            <EuiSpacer size="xs" />
            <EuiBasicTable
              items={[
                ...entities.map((e) => ({
                  name: e,
                  type: "ENTITY",
                  role: "entity",
                })),
                ...features.map((f: any) => ({
                  name: f.name,
                  type: f.valueType || "STRING",
                  role: f.name === labelerField ? "labeler" : "label",
                })),
              ]}
              columns={[
                { field: "name", name: "Field", width: "40%" },
                { field: "type", name: "Type", width: "30%" },
                {
                  field: "role",
                  name: "Role",
                  width: "30%",
                  render: (role: string) => (
                    <EuiBadge
                      color={
                        role === "entity"
                          ? "hollow"
                          : role === "labeler"
                            ? "accent"
                            : "primary"
                      }
                    >
                      {role}
                    </EuiBadge>
                  ),
                },
              ]}
              tableLayout="fixed"
              compressed
            />
          </EuiFlexItem>
          <EuiFlexItem grow={1}>
            <EuiTitle size="xxs">
              <h4>Properties</h4>
            </EuiTitle>
            <EuiSpacer size="xs" />
            <EuiFlexGroup direction="column" gutterSize="s">
              <EuiFlexItem grow={false}>
                <EuiText size="xs" color="subdued">
                  Conflict Policy
                </EuiText>
                <div>
                  <EuiBadge
                    color={
                      policyLabel === "LAST_WRITE_WINS"
                        ? "default"
                        : policyLabel === "MAJORITY_VOTE"
                          ? "primary"
                          : "accent"
                    }
                  >
                    {policyLabel}
                  </EuiBadge>
                </div>
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiText size="xs" color="subdued">
                  Labeler Field
                </EuiText>
                <EuiText size="s">
                  <strong>{labelerField}</strong>
                </EuiText>
              </EuiFlexItem>
            </EuiFlexGroup>
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>

      <EuiSpacer size="m" />

      <EuiPanel hasBorder>
        <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
          <EuiFlexItem>
            <EuiTitle size="xs">
              <h3>
                Label Records{" "}
                {allLabels && (
                  <EuiBadge color="primary">{totalEntities} total</EuiBadge>
                )}
                {searchQuery &&
                  filteredLabels.length !== (allLabels || []).length && (
                    <>
                      {" "}
                      <EuiBadge color="accent">
                        {filteredLabels.length} matching
                      </EuiBadge>
                    </>
                  )}
              </h3>
            </EuiTitle>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiButton
              size="s"
              onClick={loadLabels}
              isLoading={loadingAll}
              iconType="refresh"
            >
              Refresh
            </EuiButton>
          </EuiFlexItem>
        </EuiFlexGroup>

        <EuiHorizontalRule margin="xs" />

        <EuiText size="xs" color="subdued">
          <p>
            All label records in the online store, resolved by conflict policy.
            Use the search bar to filter by any field value.
          </p>
        </EuiText>

        <EuiSpacer size="m" />

        <EuiFieldSearch
          placeholder="Search by entity key, label value, or any field..."
          value={searchQuery}
          onChange={(e) => {
            setSearchQuery(e.target.value);
            setPageIndex(0);
          }}
          isClearable
          fullWidth
        />

        <EuiSpacer size="m" />

        {allLabels === null && loadingAll && (
          <EuiFlexGroup alignItems="center" gutterSize="s">
            <EuiFlexItem grow={false}>
              <EuiLoadingSpinner size="m" />
            </EuiFlexItem>
            <EuiFlexItem>Loading label records...</EuiFlexItem>
          </EuiFlexGroup>
        )}

        {allLabels !== null && allLabels.length === 0 && (
          <EuiEmptyPrompt
            iconType="database"
            title={<h3>No labels submitted yet</h3>}
            body="No labels have been pushed to the online store for this label view."
          />
        )}

        {allLabels !== null &&
          allLabels.length > 0 &&
          filteredLabels.length === 0 && (
            <EuiEmptyPrompt
              iconType="search"
              title={<h3>No matching records</h3>}
              body={
                <p>
                  No records match &quot;<strong>{searchQuery}</strong>&quot;.
                  Try a different search term.
                </p>
              }
            />
          )}

        {filteredLabels.length > 0 && (
          <>
            <EuiBasicTable<LabelRow>
              items={paginatedLabels}
              columns={columns}
              tableLayout="auto"
            />
            <EuiSpacer size="m" />
            <EuiTablePagination
              pageCount={Math.ceil(filteredLabels.length / pageSize)}
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
      </EuiPanel>

      {error && (
        <>
          <EuiSpacer size="m" />
          <EuiCallOut title="Error" color="danger" iconType="alert">
            {error}
          </EuiCallOut>
        </>
      )}
    </React.Fragment>
  );
};

export default LabelBrowseTab;
