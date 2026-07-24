import React, { useState, useContext, useCallback } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageTemplate,
  EuiButton,
  EuiButtonEmpty,
  EuiConfirmModal,
  EuiCallOut,
  EuiSpacer,
} from "@elastic/eui";
import { useMutation, useQueryClient } from "react-query";

import { DatasetIcon } from "../../graphics/DatasetIcon";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import DatasetOverviewTab from "./DatasetOverviewTab";
import DatasetUsageTab from "./DatasetUsageTab";
import DatasetSampleTab from "./DatasetSampleTab";
import EditDatasetModal from "./EditDatasetModal";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import {
  useDatasetCustomTabs,
  useDataSourceCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { useDataMode } from "../../contexts/DataModeContext";
import { restPost, restDelete } from "../../queries/restApiClient";
import useLoadDataset from "./useLoadDataset";

const DatasetInstance = () => {
  const navigate = useNavigate();
  const { datasetName, projectName } = useParams();

  useDocumentTitle(`${datasetName} | Datasets | Feast`);

  const { customNavigationTabs } = useDatasetCustomTabs(navigate);
  const CustomTabRoutes = useDataSourceCustomTabRoutes();

  const registryUrl = useContext(RegistryPathContext);
  const { fetchOptions } = useDataMode();
  const queryClient = useQueryClient();

  const { data: datasetData } = useLoadDataset(datasetName || "");

  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  const deleteMutation = useMutation(
    () =>
      restDelete(
        registryUrl,
        `/saved_datasets/${encodeURIComponent(datasetName || "")}?project=${encodeURIComponent(projectName || "")}`,
        fetchOptions,
      ),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(["rest", "saved-datasets-list"]);
        navigate(`/p/${projectName}/data-set`);
      },
    },
  );

  const editMutation = useMutation(
    (payload: any) =>
      restPost(registryUrl, "/saved_datasets", payload, fetchOptions),
    {
      onSuccess: () => {
        setShowEditModal(false);
        setEditError(null);
        queryClient.invalidateQueries(["rest", `saved-dataset:${datasetName}`]);
        queryClient.invalidateQueries(["rest", "saved-datasets-list"]);
      },
      onError: (err: Error) => {
        setEditError(err.message);
      },
    },
  );

  const handleEditSubmit = useCallback(
    async (payload: any) => {
      payload.project = projectName || "";
      await editMutation.mutateAsync(payload);
    },
    [projectName, editMutation],
  );

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DatasetIcon}
        pageTitle={`${datasetName}`}
        description="Saved dataset"
        rightSideItems={[
          <EuiButton
            key="edit"
            iconType="pencil"
            onClick={() => {
              setEditError(null);
              setShowEditModal(true);
            }}
          >
            Edit
          </EuiButton>,
          <EuiButtonEmpty
            key="delete"
            iconType="trash"
            color="danger"
            onClick={() => setShowDeleteConfirm(true)}
          >
            Delete
          </EuiButtonEmpty>,
        ]}
        tabs={[
          {
            label: "Overview",
            isSelected: useMatchExact(""),
            onClick: () => {
              navigate("");
            },
          },
          {
            label: "Preview Data",
            isSelected: useMatchSubpath("sample"),
            onClick: () => {
              navigate("sample");
            },
          },
          {
            label: "Usage",
            isSelected: useMatchSubpath("usage"),
            onClick: () => {
              navigate("usage");
            },
          },
          ...customNavigationTabs,
        ]}
      />
      <EuiPageTemplate.Section>
        {deleteMutation.isError && (
          <>
            <EuiCallOut
              title="Failed to delete dataset"
              color="danger"
              iconType="alert"
            >
              <p>{(deleteMutation.error as Error)?.message}</p>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}
        <Routes>
          <Route path="/" element={<DatasetOverviewTab />} />
          <Route path="/sample" element={<DatasetSampleTab />} />
          <Route path="/usage" element={<DatasetUsageTab />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>

      {showDeleteConfirm && (
        <EuiConfirmModal
          title="Delete dataset"
          onCancel={() => setShowDeleteConfirm(false)}
          onConfirm={() => deleteMutation.mutate()}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deleteMutation.isLoading}
        >
          <p>
            Are you sure you want to delete <strong>{datasetName}</strong>?
          </p>
          <p>
            This removes the dataset metadata from the registry. The underlying
            data at the storage location will not be deleted.
          </p>
        </EuiConfirmModal>
      )}

      {showEditModal && datasetData && (
        <EditDatasetModal
          dataset={datasetData}
          onClose={() => setShowEditModal(false)}
          onSubmit={handleEditSubmit}
          isSubmitting={editMutation.isLoading}
          error={editError}
        />
      )}
    </EuiPageTemplate>
  );
};

export default DatasetInstance;
