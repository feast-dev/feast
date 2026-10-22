import React, { useState } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageTemplate,
  EuiButton,
  EuiButtonEmpty,
  EuiConfirmModal,
} from "@elastic/eui";

import { FeatureServiceIcon } from "../../graphics/FeatureServiceIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import FeatureServiceOverviewTab from "./FeatureServiceOverviewTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import FeatureServiceFormModal, {
  FeatureServiceFormData,
} from "../../components/FeatureServiceFormModal";
import {
  useApplyFeatureService,
  useDeleteFeatureService,
} from "../../queries/mutations/useFeatureServiceMutations";
import useLoadFeatureService from "./useLoadFeatureService";

import {
  useFeatureServiceCustomTabs,
  useFeatureServiceCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";

const FeatureServiceInstance = () => {
  const navigate = useNavigate();
  let { featureServiceName, projectName } = useParams();

  useDocumentTitle(`${featureServiceName} | Feature Service | Feast`);

  const { customNavigationTabs } = useFeatureServiceCustomTabs(navigate);
  const CustomTabRoutes = useFeatureServiceCustomTabRoutes();

  const { data } = useLoadFeatureService(featureServiceName || "");
  const deleteFeatureService = useDeleteFeatureService();
  const applyFeatureService = useApplyFeatureService();

  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  const handleDelete = () => {
    deleteFeatureService.mutate(
      { name: featureServiceName || "", project: projectName || "" },
      {
        onSuccess: () => {
          navigate(`/p/${projectName}/feature-service`);
        },
      },
    );
  };

  const buildInitialEditData = (): FeatureServiceFormData | undefined => {
    if (!data?.spec) return undefined;
    const spec = data.spec;
    return {
      name: spec.name || featureServiceName || "",
      description: spec.description || "",
      owner: spec.owner || "",
      projections: (spec.features || []).map((proj: any) => ({
        featureViewName: proj.featureViewName || "",
        featureNames: (proj.featureColumns || [])
          .map((col: any) => col.name)
          .filter(Boolean),
      })),
      tags: Object.entries(spec.tags || {}).map(([key, value]) => ({
        key,
        value: value as string,
      })),
    };
  };

  const handleEditSubmit = (formData: FeatureServiceFormData) => {
    const payload = {
      name: formData.name,
      project: projectName || "",
      features: formData.projections.map((projection) => ({
        feature_view_name: projection.featureViewName,
        feature_names: projection.featureNames,
      })),
      description: formData.description,
      owner: formData.owner,
      tags: Object.fromEntries(
        formData.tags
          .filter((tag) => tag.key.trim())
          .map((tag) => [tag.key, tag.value]),
      ),
    };
    applyFeatureService.mutate(payload, {
      onSuccess: () => {
        setIsEditModalOpen(false);
        setEditError(null);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setEditError(message);
      },
    });
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureServiceIcon}
        pageTitle={`Feature Service: ${featureServiceName}`}
        rightSideItems={[
          <EuiButton
            key="edit"
            iconType="pencil"
            onClick={() => {
              setEditError(null);
              setIsEditModalOpen(true);
            }}
          >
            Edit
          </EuiButton>,
          <EuiButtonEmpty
            key="delete"
            color="danger"
            iconType="trash"
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
          ...customNavigationTabs,
        ]}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route path="/" element={<FeatureServiceOverviewTab />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>

      {showDeleteConfirm && (
        <EuiConfirmModal
          title={`Delete "${featureServiceName}"?`}
          onCancel={() => setShowDeleteConfirm(false)}
          onConfirm={handleDelete}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deleteFeatureService.isLoading}
        >
          <p>
            This will permanently remove the feature service. This action cannot
            be undone.
          </p>
        </EuiConfirmModal>
      )}

      {isEditModalOpen && (
        <FeatureServiceFormModal
          onClose={() => {
            setIsEditModalOpen(false);
            setEditError(null);
          }}
          onSubmit={handleEditSubmit}
          initialData={buildInitialEditData()}
          isEdit={true}
          isSubmitting={applyFeatureService.isLoading}
          submitError={editError}
        />
      )}
    </EuiPageTemplate>
  );
};

export default FeatureServiceInstance;
