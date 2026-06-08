import React, { useContext, useState, useMemo, useCallback } from "react";
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
  EuiStat,
  EuiBadge,
  EuiBasicTable,
  EuiBasicTableColumn,
  EuiCodeBlock,
  EuiIcon,
  EuiEmptyPrompt,
  EuiFieldNumber,
  EuiSuperSelect,
  EuiFieldSearch,
  EuiTablePagination,
  EuiModal,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiModalBody,
  EuiModalFooter,
  EuiOverlayMask,
  EuiGlobalToastList,
} from "@elastic/eui";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadLabelView from "./useLoadLabelView";
import useLoadRegistry from "../../queries/useLoadRegistry";

interface CandidateData {
  unlabeled_entities: Record<string, any>[];
  total_labeled: number;
  total_unlabeled: number;
  entity_names: string[];
  feature_names: string[];
  label_view: string;
  reference_feature_view: string | null;
}

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

const ActiveLearningTab = () => {
  const { labelViewName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const name = labelViewName || "";
  const { isLoading, data } = useLoadLabelView(name);
  const { data: registryData } = useLoadRegistry(registryUrl);

  const [refFV, setRefFV] = useState("");
  const [limit, setLimit] = useState(50);
  const [candidates, setCandidates] = useState<CandidateData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedItems, setSelectedItems] = useState<Record<string, any>[]>([]);
  const [labelValues, setLabelValues] = useState<Record<string, string>>({});
  const [submitting, setSubmitting] = useState(false);
  const [submitSuccess, setSubmitSuccess] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(25);
  const [toasts, setToasts] = useState<
    Array<{
      id: string;
      title: string;
      color: "success" | "danger";
      iconType: string;
    }>
  >([]);

  const removeToast = useCallback((removedToast: { id: string }) => {
    setToasts((prev) => prev.filter((t) => t.id !== removedToast.id));
  }, []);

  const spec = data?.object?.spec || data?.spec || {};
  const labelFields: { name: string; valueType?: string }[] =
    spec.features || [];
  const labelerField: string | null =
    spec.labelerField || spec.labeler_field || null;

  const featureViewOptions = (registryData?.objects?.featureViews || [])
    .filter((fv: any) => {
      const fvName = fv.spec?.name || fv.name || "";
      return fvName !== name;
    })
    .map((fv: any) => ({
      value: fv.spec?.name || fv.name || "",
      inputDisplay: fv.spec?.name || fv.name || "Unknown",
      dropdownDisplay: (
        <span>
          <strong>{fv.spec?.name || fv.name}</strong>
          {fv.spec?.description && (
            <EuiText size="xs" color="subdued">
              <p style={{ margin: 0 }}>{fv.spec.description}</p>
            </EuiText>
          )}
        </span>
      ),
    }));

  const fetchCandidates = async () => {
    setLoading(true);
    setError(null);
    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const response = await fetch(`${baseUrl}/active-learning/candidates`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          feature_view: name,
          reference_feature_view: refFV || undefined,
          limit: limit,
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
              : "Failed to fetch candidates",
        );
      } else {
        setCandidates(result);
      }
    } catch (e: any) {
      setError(e.message || "Network error");
    } finally {
      setLoading(false);
    }
  };

  const entityColumns: EuiBasicTableColumn<Record<string, any>>[] = candidates
    ? Object.keys(candidates.unlabeled_entities[0] || {}).map((key) => ({
        field: key,
        name: key,
        sortable: true,
      }))
    : [];

  const filteredCandidates = useMemo(() => {
    if (!candidates) return [];
    if (!searchQuery.trim()) return candidates.unlabeled_entities;
    const query = searchQuery.toLowerCase();
    return candidates.unlabeled_entities.filter((row) =>
      Object.values(row).some(
        (val) => val != null && String(val).toLowerCase().includes(query),
      ),
    );
  }, [candidates, searchQuery]);

  const paginatedCandidates = useMemo(() => {
    const start = pageIndex * pageSize;
    return filteredCandidates.slice(start, start + pageSize);
  }, [filteredCandidates, pageIndex, pageSize]);

  const handleSubmitLabels = async () => {
    if (selectedItems.length === 0) return;
    setSubmitting(true);
    setSubmitSuccess(null);
    setError(null);

    try {
      const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";
      const pushSourceName =
        spec.source?.pushSourceName ||
        spec.source?.name ||
        `${name}_push_source`;

      const rows = selectedItems.map((entity) => {
        const row: Record<string, any> = { ...entity, ...labelValues };
        row["event_timestamp"] = new Date().toISOString();
        return row;
      });

      const columnar: Record<string, any[]> = {};
      if (rows.length > 0) {
        for (const key of Object.keys(rows[0])) {
          columnar[key] = rows.map((r) => r[key]);
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
        const msg = `Successfully labeled ${selectedItems.length} record${selectedItems.length !== 1 ? "s" : ""}`;
        setSubmitSuccess(msg);
        setToasts((prev) => [
          ...prev,
          {
            id: String(Date.now()),
            title: msg,
            color: "success",
            iconType: "check",
          },
        ]);
        setSelectedItems([]);
        setLabelValues({});
        fetchCandidates();
      } else {
        const errData = await response.json().catch(() => null);
        const detail = errData?.detail;
        setError(
          typeof detail === "string"
            ? detail
            : Array.isArray(detail)
              ? detail.map((d: any) => d.msg || JSON.stringify(d)).join("; ")
              : `Label submission failed (${response.status})`,
        );
      }
    } catch (e: any) {
      setError(e.message || "Network error during label submission");
    } finally {
      setSubmitting(false);
    }
  };

  if (isLoading) {
    return (
      <p>
        <EuiLoadingSpinner size="m" /> Loading...
      </p>
    );
  }

  const exportJSON = () => {
    if (!candidates) return;
    const blob = new Blob(
      [JSON.stringify(candidates.unlabeled_entities, null, 2)],
      { type: "application/json" },
    );
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${name}_unlabeled_candidates.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const argillaPushCode = candidates
    ? `import argilla as rg
import requests

# Fetch unlabeled candidates from Feast
response = requests.post(
    "${window.location.origin}/api/v1/active-learning/candidates",
    json={
        "feature_view": "${name}",
        ${refFV ? `"reference_feature_view": "${refFV}",` : ""}
        "limit": ${limit},
    },
)
candidates = response.json()["unlabeled_entities"]

# Create Argilla records for annotation
records = []
for entity in candidates:
    records.append(
        rg.Record(
            fields={"text": f"Review entity: {entity}"},
            metadata=entity,
            suggestions=[],  # Add model predictions here for pre-labeling
        )
    )

# Push to Argilla dataset for human annotation
dataset = rg.Dataset(name="${name}_annotation", settings=your_settings)
dataset.records.log(records)
print(f"Pushed {len(records)} candidates for annotation")`
    : "";

  return (
    <React.Fragment>
      <EuiCallOut
        title="Active Learning Candidates"
        color="primary"
        iconType="training"
      >
        <EuiText size="s">
          Find records that exist in your feature views but have NOT been
          labeled yet. Label them directly here in the Feast UI, or export
          candidates to your annotation tool (Argilla, Label Studio) for
          targeted human review.
        </EuiText>
      </EuiCallOut>

      <EuiSpacer size="l" />

      <EuiPanel>
        <EuiTitle size="xs">
          <h3>Find Unlabeled Records</h3>
        </EuiTitle>
        <EuiSpacer size="m" />

        <EuiForm>
          <EuiFormRow
            label="Reference Feature View"
            helpText="A Feature View that holds your full set of records (e.g. user_profile). Used to find which records don't have labels yet."
          >
            {featureViewOptions.length > 0 ? (
              <EuiSuperSelect
                options={featureViewOptions}
                valueOfSelected={refFV}
                onChange={(value) => setRefFV(value)}
                placeholder="Select a feature view..."
                hasDividers
              />
            ) : (
              <EuiFieldText
                placeholder="e.g. user_profile or loan_features"
                value={refFV}
                onChange={(e) => setRefFV(e.target.value)}
              />
            )}
          </EuiFormRow>

          <EuiFormRow label="Max candidates">
            <EuiFieldNumber
              min={1}
              max={1000}
              value={limit}
              onChange={(e) => setLimit(parseInt(e.target.value) || 50)}
            />
          </EuiFormRow>

          <EuiSpacer size="m" />

          <EuiButton
            fill
            onClick={fetchCandidates}
            isLoading={loading}
            iconType="search"
          >
            Find Unlabeled Records
          </EuiButton>
        </EuiForm>
      </EuiPanel>

      {error && !isModalOpen && (
        <React.Fragment>
          <EuiSpacer size="m" />
          <EuiCallOut title="Error" color="danger" iconType="alert">
            {error}
          </EuiCallOut>
        </React.Fragment>
      )}

      {candidates && (
        <React.Fragment>
          <EuiSpacer size="l" />

          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiPanel>
                <EuiStat
                  title={candidates.total_labeled.toString()}
                  description="Already Labeled Records"
                  titleColor="success"
                />
              </EuiPanel>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiPanel>
                <EuiStat
                  title={candidates.total_unlabeled.toString()}
                  description="Unlabeled Records"
                  titleColor="danger"
                />
              </EuiPanel>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiPanel>
                <EuiStat
                  title={
                    candidates.total_labeled + candidates.total_unlabeled > 0
                      ? `${(
                          (candidates.total_labeled /
                            (candidates.total_labeled +
                              candidates.total_unlabeled)) *
                          100
                        ).toFixed(1)}%`
                      : "N/A"
                  }
                  description="Label Coverage"
                  titleColor="primary"
                />
              </EuiPanel>
            </EuiFlexItem>
          </EuiFlexGroup>

          <EuiSpacer size="l" />

          {candidates.unlabeled_entities.length > 0 ? (
            <React.Fragment>
              <EuiPanel>
                <EuiFlexGroup alignItems="center">
                  <EuiFlexItem>
                    <EuiTitle size="xs">
                      <h3>
                        Unlabeled Records{" "}
                        <EuiBadge color="danger">
                          {candidates.unlabeled_entities.length}
                        </EuiBadge>
                        {selectedItems.length > 0 && (
                          <>
                            {" "}
                            <EuiBadge color="primary">
                              {selectedItems.length} selected
                            </EuiBadge>
                          </>
                        )}
                      </h3>
                    </EuiTitle>
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiFlexGroup gutterSize="s">
                      {selectedItems.length > 0 && (
                        <EuiFlexItem grow={false}>
                          <EuiButton
                            size="s"
                            fill
                            color="success"
                            onClick={() => setIsModalOpen(true)}
                            iconType="tag"
                          >
                            Label Selected ({selectedItems.length})
                          </EuiButton>
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
                    </EuiFlexGroup>
                  </EuiFlexItem>
                </EuiFlexGroup>
                <EuiSpacer size="s" />
                <EuiText size="xs" color="subdued">
                  Select records below, then click &quot;Label Selected&quot; to
                  assign labels.
                </EuiText>
                <EuiSpacer size="s" />
                <EuiFieldSearch
                  placeholder="Search candidates..."
                  value={searchQuery}
                  onChange={(e) => {
                    setSearchQuery(e.target.value);
                    setPageIndex(0);
                  }}
                  isClearable
                  fullWidth
                />
                <EuiSpacer size="m" />
                <EuiBasicTable
                  items={paginatedCandidates}
                  columns={entityColumns}
                  tableLayout="auto"
                  itemId={(item: Record<string, any>) =>
                    Object.values(item).join("_")
                  }
                  selection={{
                    onSelectionChange: (items: Record<string, any>[]) =>
                      setSelectedItems(items),
                    selectable: () => true,
                    selectableMessage: () => "Select to label",
                  }}
                />
                {filteredCandidates.length > pageSize && (
                  <>
                    <EuiSpacer size="m" />
                    <EuiTablePagination
                      pageCount={Math.ceil(
                        filteredCandidates.length / pageSize,
                      )}
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

              {submitSuccess && (
                <React.Fragment>
                  <EuiSpacer size="m" />
                  <EuiCallOut
                    title={submitSuccess}
                    color="success"
                    iconType="check"
                  />
                </React.Fragment>
              )}

              {isModalOpen && (
                <EuiOverlayMask>
                  <EuiModal
                    onClose={() => setIsModalOpen(false)}
                    maxWidth={500}
                  >
                    <EuiModalHeader>
                      <EuiModalHeaderTitle>
                        <EuiIcon type="tag" /> Label {selectedItems.length}{" "}
                        Record
                        {selectedItems.length !== 1 ? "s" : ""}
                      </EuiModalHeaderTitle>
                    </EuiModalHeader>
                    <EuiModalBody>
                      <EuiText size="s" color="subdued">
                        Assign label values to all {selectedItems.length}{" "}
                        selected records. Labels will be pushed via{" "}
                        <code>FeatureStore.push()</code>.
                      </EuiText>
                      <EuiSpacer size="m" />
                      <EuiForm>
                        {labelFields
                          .filter((f) => f.name !== labelerField)
                          .map((field) => (
                            <EuiFormRow
                              key={field.name}
                              label={`${field.name}${field.valueType ? ` (${field.valueType})` : ""}`}
                            >
                              <EuiFieldText
                                placeholder={`Enter ${field.name} value`}
                                value={labelValues[field.name] || ""}
                                onChange={(e) =>
                                  setLabelValues((prev) => ({
                                    ...prev,
                                    [field.name]: e.target.value,
                                  }))
                                }
                              />
                            </EuiFormRow>
                          ))}
                        {labelerField && (
                          <EuiFormRow label={`${labelerField} (your identity)`}>
                            <EuiFieldText
                              placeholder="e.g. your_name"
                              value={labelValues[labelerField] || ""}
                              onChange={(e) =>
                                setLabelValues((prev) => ({
                                  ...prev,
                                  [labelerField]: e.target.value,
                                }))
                              }
                            />
                          </EuiFormRow>
                        )}
                      </EuiForm>
                      {error && (
                        <React.Fragment>
                          <EuiSpacer size="m" />
                          <EuiCallOut
                            title="Submission Error"
                            color="danger"
                            iconType="alert"
                            size="s"
                          >
                            {error}
                          </EuiCallOut>
                        </React.Fragment>
                      )}
                    </EuiModalBody>
                    <EuiModalFooter>
                      <EuiButton
                        color="text"
                        onClick={() => {
                          setIsModalOpen(false);
                          setLabelValues({});
                        }}
                      >
                        Cancel
                      </EuiButton>
                      <EuiButton
                        fill
                        color="success"
                        onClick={async () => {
                          await handleSubmitLabels();
                          if (!error) {
                            setIsModalOpen(false);
                          }
                        }}
                        isLoading={submitting}
                        iconType="check"
                      >
                        Submit Labels
                      </EuiButton>
                    </EuiModalFooter>
                  </EuiModal>
                </EuiOverlayMask>
              )}
            </React.Fragment>
          ) : (
            <EuiEmptyPrompt
              iconType="check"
              title={<h3>All records are labeled!</h3>}
              body="No unlabeled records found in the reference feature view."
            />
          )}

          <EuiSpacer size="l" />

          <EuiPanel color="subdued">
            <EuiTitle size="xxs">
              <h4>Push Candidates to Argilla</h4>
            </EuiTitle>
            <EuiSpacer size="s" />
            <EuiText size="xs" color="subdued">
              Use this script to push unlabeled candidates to Argilla for
              annotation:
            </EuiText>
            <EuiSpacer size="s" />
            <EuiCodeBlock language="python" paddingSize="s" isCopyable>
              {argillaPushCode}
            </EuiCodeBlock>
          </EuiPanel>
        </React.Fragment>
      )}

      <div
        className="toast-top-right"
        style={{ position: "fixed", top: 16, right: 16, zIndex: 9999 }}
      >
        <EuiGlobalToastList
          toasts={toasts}
          dismissToast={removeToast}
          toastLifeTimeMs={5000}
        />
      </div>
    </React.Fragment>
  );
};

export default ActiveLearningTab;
