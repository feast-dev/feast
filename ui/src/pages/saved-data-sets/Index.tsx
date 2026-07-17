import React, {
  useState,
  useContext,
  useCallback,
  useMemo,
  useEffect,
} from "react";
import { useParams } from "react-router-dom";
import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiButton,
  EuiSpacer,
  EuiConfirmModal,
  EuiCallOut,
  EuiFlexGroup,
  EuiFlexItem,
  EuiFieldSearch,
  EuiStat,
  EuiPanel,
  EuiSelect,
  EuiText,
  EuiButtonGroup,
  EuiBadge,
  EuiAccordion,
  EuiIcon,
  EuiToolTip,
} from "@elastic/eui";
import { useMutation, useQuery, useQueryClient } from "react-query";

import { DatasetIcon } from "../../graphics/DatasetIcon";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DatasetsCardGrid from "./DatasetsCardGrid";
import DatasetsListingTable from "./DatasetsListingTable";
import DatasetsIndexEmptyState from "./DatasetsIndexEmptyState";
import AddToCatalogModal from "./AddToCatalogModal";
import type { RegisterDatasetPayload } from "./RegisterDatasetModal";
import ExportButton from "../../components/ExportButton";
import useResourceQuery, {
  savedDatasetListPath,
} from "../../queries/useResourceQuery";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { useDataMode } from "../../contexts/DataModeContext";
import { restPost, restDelete } from "../../queries/restApiClient";

const useLoadSavedDataSets = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "saved-datasets-list",
    project: projectName,
    restPath: savedDatasetListPath(projectName),
    restSelect: (d) => d.savedDatasets,
  });
};

const SORT_OPTIONS = [
  { value: "name_asc", text: "Name (A → Z)" },
  { value: "name_desc", text: "Name (Z → A)" },
  { value: "created_desc", text: "Newest first" },
  { value: "created_asc", text: "Oldest first" },
  { value: "features_desc", text: "Most features" },
];

const VIEW_TOGGLE_BUTTONS = [
  { id: "cards", label: "Cards", iconType: "grid" },
  { id: "table", label: "Table", iconType: "list" },
];

function getDatasetSortValue(dataset: any, key: string): any {
  const spec = dataset.spec || dataset;
  const meta = dataset.meta || {};
  if (key === "name") return (spec.name || "").toLowerCase();
  if (key === "created")
    return meta.createdTimestamp || meta.created_timestamp || "";
  if (key === "features") return (spec.features || []).length;
  return "";
}

const Index = () => {
  const { projectName } = useParams();
  const { isLoading, isSuccess, isError, data } = useLoadSavedDataSets();
  const [showRegisterModal, setShowRegisterModal] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [sortBy, setSortBy] = useState("created_desc");
  const [viewMode, setViewMode] = useState("cards");
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();
  const queryClient = useQueryClient();

  useDocumentTitle(`Data Catalog | Feast`);

  const registerMutation = useMutation(
    (payload: RegisterDatasetPayload) =>
      restPost(registryUrl, "/saved_datasets", payload, fetchOptions),
    {
      onSuccess: (_: any, variables: RegisterDatasetPayload) => {
        setShowRegisterModal(false);
        setSubmitError(null);
        setSuccessMessage(
          `Dataset "${variables.name}" registered successfully.`,
        );
        setTimeout(() => setSuccessMessage(null), 5000);
        queryClient.invalidateQueries(["rest", "saved-datasets-list"]);
      },
      onError: (err: Error) => {
        setSubmitError(err.message);
      },
    },
  );

  const deleteMutation = useMutation(
    (name: string) =>
      restDelete(
        registryUrl,
        `/saved_datasets/${encodeURIComponent(name)}?project=${encodeURIComponent(projectName || "")}`,
        fetchOptions,
      ),
    {
      onSuccess: () => {
        setDeleteTarget(null);
        queryClient.invalidateQueries(["rest", "saved-datasets-list"]);
      },
    },
  );

  // Poll active jobs
  const { data: jobsData } = useQuery(
    ["dataset-jobs", projectName],
    async () => {
      const response = await fetch(
        `${registryUrl}/saved_datasets/jobs?project=${encodeURIComponent(projectName || "")}`,
        fetchOptions,
      );
      if (!response.ok) return { jobs: [] };
      return response.json();
    },
    {
      enabled: !!registryUrl,
      refetchInterval: 5000,
      staleTime: 3000,
    },
  );

  const activeJobs = useMemo(
    () =>
      (jobsData?.jobs || []).filter(
        (j: any) => j.status === "pending" || j.status === "running",
      ),
    [jobsData],
  );

  const recentJobs = useMemo(
    () => (jobsData?.jobs || []).slice(0, 10),
    [jobsData],
  );

  // Toast for completed jobs
  useEffect(() => {
    const completedJobs = (jobsData?.jobs || []).filter(
      (j: any) => j.status === "completed",
    );
    if (completedJobs.length > 0) {
      const latest = completedJobs[0];
      if (latest.completed_at) {
        const completedTime = new Date(latest.completed_at).getTime();
        const now = Date.now();
        if (now - completedTime < 10000) {
          setSuccessMessage(
            `Dataset "${latest.dataset_name}" created successfully.`,
          );
          setTimeout(() => setSuccessMessage(null), 5000);
          queryClient.invalidateQueries(["rest", "saved-datasets-list"]);
        }
      }
    }
  }, [jobsData, queryClient]);

  const handleRegisterSubmit = useCallback(
    async (payload: RegisterDatasetPayload) => {
      payload.project = projectName || "";
      await registerMutation.mutateAsync(payload);
    },
    [projectName, registerMutation],
  );

  const handleDeleteConfirm = useCallback(() => {
    if (deleteTarget) {
      deleteMutation.mutate(deleteTarget);
    }
  }, [deleteTarget, deleteMutation]);

  // Compute summary stats
  const stats = useMemo(() => {
    if (!data)
      return { total: 0, totalFeatures: 0, storageTypes: new Set<string>() };
    const totalFeatures = data.reduce(
      (acc: number, ds: any) => acc + (ds.spec?.features?.length || 0),
      0,
    );
    const storageTypes = new Set<string>();
    data.forEach((ds: any) => {
      const storage = ds.spec?.storage;
      if (storage?.fileStorage) storageTypes.add("File");
      else if (storage?.bigqueryStorage) storageTypes.add("BigQuery");
      else if (storage?.snowflakeStorage) storageTypes.add("Snowflake");
      else if (storage?.redshiftStorage) storageTypes.add("Redshift");
      else if (storage?.sparkStorage) storageTypes.add("Spark");
      else if (storage?.trinoStorage) storageTypes.add("Trino");
      else if (storage?.athenaStorage) storageTypes.add("Athena");
      else if (storage?.customStorage) storageTypes.add("Custom");
    });
    return { total: data.length, totalFeatures, storageTypes };
  }, [data]);

  // Filter and sort
  const processedData = useMemo(() => {
    if (!data) return [];
    let filtered = data;

    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      filtered = data.filter((ds: any) => {
        const name = (ds.spec?.name || "").toLowerCase();
        const tags = JSON.stringify(ds.spec?.tags || {}).toLowerCase();
        const features = (ds.spec?.features || []).join(" ").toLowerCase();
        const service = (
          ds.spec?.featureServiceName ||
          ds.spec?.feature_service_name ||
          ""
        ).toLowerCase();
        return (
          name.includes(q) ||
          tags.includes(q) ||
          features.includes(q) ||
          service.includes(q)
        );
      });
    }

    const [key, order] = sortBy.split("_");
    filtered = [...filtered].sort((a, b) => {
      const aVal = getDatasetSortValue(a, key);
      const bVal = getDatasetSortValue(b, key);
      if (typeof aVal === "number" && typeof bVal === "number") {
        return order === "asc" ? aVal - bVal : bVal - aVal;
      }
      const cmp = String(aVal).localeCompare(String(bVal));
      return order === "asc" ? cmp : -cmp;
    });

    return filtered;
  }, [data, searchQuery, sortBy]);

  const hasData = data && data.length > 0;

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DatasetIcon}
        pageTitle="Data Catalog"
        rightSideItems={[
          <EuiFlexGroup
            key="actions"
            gutterSize="s"
            alignItems="center"
            responsive={false}
          >
            {activeJobs.length > 0 && (
              <EuiFlexItem grow={false}>
                <EuiToolTip content={`${activeJobs.length} job(s) running`}>
                  <EuiBadge color="primary" iconType="clock">
                    {activeJobs.length} active
                  </EuiBadge>
                </EuiToolTip>
              </EuiFlexItem>
            )}
            <EuiFlexItem grow={false}>
              <EuiButton
                fill
                iconType="plus"
                onClick={() => {
                  setSubmitError(null);
                  setShowRegisterModal(true);
                }}
              >
                Add to Catalog
              </EuiButton>
            </EuiFlexItem>
          </EuiFlexGroup>,
          <ExportButton
            data={processedData ?? []}
            fileName="datasets"
            formats={["json"]}
            key="export"
          />,
        ]}
      />
      <EuiPageTemplate.Section>
        {/* Success toast */}
        {successMessage && (
          <>
            <EuiCallOut
              title={successMessage}
              color="success"
              iconType="check"
              size="s"
            />
            <EuiSpacer size="m" />
          </>
        )}

        {/* Active Jobs Panel */}
        {recentJobs.length > 0 && (
          <>
            <EuiAccordion
              id="dataset-jobs-accordion"
              buttonContent={
                <EuiFlexGroup
                  gutterSize="s"
                  alignItems="center"
                  responsive={false}
                >
                  <EuiFlexItem grow={false}>
                    <EuiIcon type="timeline" />
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiText size="xs">
                      <strong>Recent Activity</strong>
                      {activeJobs.length > 0 && (
                        <EuiBadge color="primary" style={{ marginLeft: 8 }}>
                          {activeJobs.length} running
                        </EuiBadge>
                      )}
                    </EuiText>
                  </EuiFlexItem>
                </EuiFlexGroup>
              }
              paddingSize="s"
              initialIsOpen={activeJobs.length > 0}
            >
              <EuiPanel color="subdued" paddingSize="s">
                {recentJobs.map((job: any) => (
                  <EuiFlexGroup
                    key={job.job_id}
                    gutterSize="s"
                    alignItems="center"
                    responsive={false}
                    style={{ padding: "4px 0" }}
                  >
                    <EuiFlexItem grow={false}>
                      {job.status === "running" || job.status === "pending" ? (
                        <EuiLoadingSpinner size="s" />
                      ) : job.status === "completed" ? (
                        <EuiIcon type="check" color="success" />
                      ) : (
                        <EuiIcon type="alert" color="danger" />
                      )}
                    </EuiFlexItem>
                    <EuiFlexItem>
                      <EuiText size="xs">
                        <strong>{job.dataset_name}</strong>
                      </EuiText>
                    </EuiFlexItem>
                    <EuiFlexItem grow={false}>
                      <EuiBadge
                        color={
                          job.status === "completed"
                            ? "success"
                            : job.status === "failed"
                              ? "danger"
                              : "primary"
                        }
                      >
                        {job.status}
                      </EuiBadge>
                    </EuiFlexItem>
                  </EuiFlexGroup>
                ))}
              </EuiPanel>
            </EuiAccordion>
            <EuiSpacer size="m" />
          </>
        )}

        {/* Delete error */}
        {deleteMutation.isError && (
          <>
            <EuiCallOut
              title="Failed to delete dataset"
              color="danger"
              iconType="alert"
              size="s"
            >
              <p>{(deleteMutation.error as Error)?.message}</p>
            </EuiCallOut>
            <EuiSpacer size="m" />
          </>
        )}

        {isLoading && (
          <EuiFlexGroup justifyContent="center" alignItems="center">
            <EuiFlexItem grow={false}>
              <EuiLoadingSpinner size="l" />
            </EuiFlexItem>
            <EuiFlexItem grow={false}>
              <EuiText>Loading datasets...</EuiText>
            </EuiFlexItem>
          </EuiFlexGroup>
        )}

        {isError && (
          <EuiCallOut
            title="Failed to load datasets"
            color="danger"
            iconType="alert"
          >
            <p>
              We encountered an error while loading datasets. Please check that
              the registry server is running.
            </p>
          </EuiCallOut>
        )}

        {isSuccess && hasData && (
          <>
            {/* Summary Stats */}
            <EuiFlexGroup gutterSize="l">
              <EuiFlexItem>
                <EuiPanel hasBorder paddingSize="m">
                  <EuiStat
                    title={stats.total}
                    description="Datasets"
                    titleSize="m"
                    reverse
                  />
                </EuiPanel>
              </EuiFlexItem>
              <EuiFlexItem>
                <EuiPanel hasBorder paddingSize="m">
                  <EuiStat
                    title={stats.totalFeatures}
                    description="Total Features"
                    titleSize="m"
                    reverse
                  />
                </EuiPanel>
              </EuiFlexItem>
              <EuiFlexItem>
                <EuiPanel hasBorder paddingSize="m">
                  <EuiStat
                    title={stats.storageTypes.size}
                    description="Storage Backends"
                    titleSize="m"
                    reverse
                  />
                </EuiPanel>
              </EuiFlexItem>
            </EuiFlexGroup>

            <EuiSpacer size="l" />

            {/* Search + Sort + View Toggle */}
            <EuiFlexGroup alignItems="center" gutterSize="m">
              <EuiFlexItem>
                <EuiFieldSearch
                  placeholder="Search by name, features, tags, or service..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  isClearable
                  fullWidth
                />
              </EuiFlexItem>
              <EuiFlexItem grow={false} style={{ width: 180 }}>
                <EuiSelect
                  options={SORT_OPTIONS}
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                  compressed
                  prepend="Sort"
                />
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiButtonGroup
                  legend="View mode"
                  options={VIEW_TOGGLE_BUTTONS}
                  idSelected={viewMode}
                  onChange={(id) => setViewMode(id)}
                  isIconOnly
                  buttonSize="compressed"
                />
              </EuiFlexItem>
            </EuiFlexGroup>

            <EuiSpacer size="l" />

            {/* Results count when searching */}
            {searchQuery.trim() && (
              <>
                <EuiText size="xs" color="subdued">
                  Showing {processedData.length} of {data.length} datasets
                </EuiText>
                <EuiSpacer size="s" />
              </>
            )}

            {/* Content */}
            {viewMode === "cards" ? (
              <DatasetsCardGrid
                datasets={processedData}
                onDelete={(name) => setDeleteTarget(name)}
              />
            ) : (
              <DatasetsListingTable datasets={processedData} />
            )}
          </>
        )}

        {isSuccess && !hasData && (
          <DatasetsIndexEmptyState
            onRegister={() => {
              setSubmitError(null);
              setShowRegisterModal(true);
            }}
          />
        )}
      </EuiPageTemplate.Section>

      {showRegisterModal && (
        <AddToCatalogModal
          onClose={() => setShowRegisterModal(false)}
          onLinkSubmit={handleRegisterSubmit}
          isLinkSubmitting={registerMutation.isLoading}
          linkError={submitError}
        />
      )}

      {deleteTarget && (
        <EuiConfirmModal
          title="Delete dataset"
          onCancel={() => setDeleteTarget(null)}
          onConfirm={handleDeleteConfirm}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deleteMutation.isLoading}
        >
          <p>
            Are you sure you want to delete <strong>{deleteTarget}</strong>?
          </p>
          <p>
            <EuiText size="s" color="subdued">
              This removes the dataset metadata from the registry. The
              underlying data at the storage location will not be deleted.
            </EuiText>
          </p>
        </EuiConfirmModal>
      )}
    </EuiPageTemplate>
  );
};

export default Index;
