import React, { useContext, useState } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiBadge,
  EuiButton,
  EuiButtonEmpty,
  EuiConfirmModal,
  EuiPageTemplate,
} from "@elastic/eui";

import { FeatureViewIcon } from "../../graphics/FeatureViewIcon";

import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import RegularFeatureViewOverviewTab from "./RegularFeatureViewOverviewTab";
import FeatureViewLineageTab from "./FeatureViewLineageTab";
import FeatureViewVersionsTab from "./FeatureViewVersionsTab";
import FeatureViewFormModal, {
  FeatureViewFormData,
} from "../../components/FeatureViewFormModal";
import {
  useApplyFeatureView,
  useDeleteFeatureView,
} from "../../queries/mutations/useFeatureViewMutations";

import {
  useRegularFeatureViewCustomTabs,
  useRegularFeatureViewCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";
import FeatureFlagsContext from "../../contexts/FeatureFlagsContext";
import { feast } from "../../protos";

interface RegularFeatureInstanceProps {
  data: feast.core.IFeatureView;
  permissions?: any[];
}

const buildEditFormData = (
  fv: feast.core.IFeatureView,
): FeatureViewFormData => {
  const tags = fv.spec?.tags
    ? Object.entries(fv.spec.tags).map(([key, value]) => ({ key, value }))
    : [];

  const features = (fv.spec?.features || []).map((f) => ({
    name: f.name || "",
    valueType: String(f.valueType ?? 0),
    description: f.description || "",
  }));

  let ttlValue = 0;
  let ttlUnit = "seconds";
  if (fv.spec?.ttl?.seconds) {
    const secs =
      typeof fv.spec.ttl.seconds === "number"
        ? fv.spec.ttl.seconds
        : ((fv.spec.ttl.seconds as any).toNumber?.() ?? 0);
    if (secs > 0 && secs % 86400 === 0) {
      ttlValue = secs / 86400;
      ttlUnit = "days";
    } else if (secs > 0 && secs % 3600 === 0) {
      ttlValue = secs / 3600;
      ttlUnit = "hours";
    } else if (secs > 0 && secs % 60 === 0) {
      ttlValue = secs / 60;
      ttlUnit = "minutes";
    } else {
      ttlValue = secs;
      ttlUnit = "seconds";
    }
  }

  return {
    name: fv.spec?.name || "",
    description: fv.spec?.description || "",
    owner: fv.spec?.owner || "",
    entities: fv.spec?.entities || [],
    features,
    batchSource: fv.spec?.batchSource?.name || "",
    ttlValue,
    ttlUnit,
    online: fv.spec?.online ?? true,
    tags,
  };
};

const TTL_UNITS: Record<string, number> = {
  days: 86400,
  hours: 3600,
  minutes: 60,
  seconds: 1,
};

const RegularFeatureInstance = ({
  data,
  permissions,
}: RegularFeatureInstanceProps) => {
  const { enabledFeatureStatistics } = useContext(FeatureFlagsContext);
  const navigate = useNavigate();
  const { projectName } = useParams();

  const { customNavigationTabs } = useRegularFeatureViewCustomTabs(navigate);
  let tabs = [
    {
      label: "Overview",
      isSelected: useMatchExact(""),
      onClick: () => {
        navigate("");
      },
    },
  ];

  tabs.push({
    label: "Lineage",
    isSelected: useMatchSubpath("lineage"),
    onClick: () => {
      navigate("lineage");
    },
  });

  let statisticsIsSelected = useMatchSubpath("statistics");
  if (enabledFeatureStatistics) {
    tabs.push({
      label: "Statistics",
      isSelected: statisticsIsSelected,
      onClick: () => {
        navigate("statistics");
      },
    });
  }

  tabs.push({
    label: "Versions",
    isSelected: useMatchSubpath("versions"),
    onClick: () => {
      navigate("versions");
    },
  });

  tabs.push(...customNavigationTabs);

  const TabRoutes = useRegularFeatureViewCustomTabRoutes();

  const applyFeatureView = useApplyFeatureView();
  const deleteFeatureView = useDeleteFeatureView();

  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  const handleDelete = () => {
    deleteFeatureView.mutate(
      { name: data?.spec?.name || "", project: projectName || "" },
      {
        onSuccess: () => {
          navigate(`/p/${projectName}/feature-view`);
        },
      },
    );
  };

  const handleEditSubmit = (formData: FeatureViewFormData) => {
    const payload = {
      name: formData.name,
      project: projectName || "",
      entities: formData.entities,
      features: formData.features.map((f) => ({
        name: f.name,
        value_type: parseInt(f.valueType, 10),
        description: f.description,
      })),
      batch_source: formData.batchSource,
      ttl_seconds: formData.ttlValue * (TTL_UNITS[formData.ttlUnit] || 1),
      online: formData.online,
      description: formData.description,
      owner: formData.owner,
      tags: Object.fromEntries(
        formData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
      ),
    };
    applyFeatureView.mutate(payload, {
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
        iconType={FeatureViewIcon}
        pageTitle={
          <>
            {data?.spec?.name}
            {data?.meta?.currentVersionNumber != null &&
              data.meta.currentVersionNumber > 0 && (
                <EuiBadge color="hollow" style={{ marginLeft: 8 }}>
                  v{data.meta.currentVersionNumber}
                </EuiBadge>
              )}
          </>
        }
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
        tabs={tabs}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route
            path="/"
            element={
              <RegularFeatureViewOverviewTab
                data={data}
                permissions={permissions}
              />
            }
          />
          <Route
            path="/lineage"
            element={<FeatureViewLineageTab data={data} />}
          />
          <Route
            path="/versions"
            element={
              <FeatureViewVersionsTab featureViewName={data?.spec?.name!} />
            }
          />
          {TabRoutes}
        </Routes>
      </EuiPageTemplate.Section>

      {showDeleteConfirm && (
        <EuiConfirmModal
          title={`Delete "${data?.spec?.name}"?`}
          onCancel={() => setShowDeleteConfirm(false)}
          onConfirm={handleDelete}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deleteFeatureView.isLoading}
        >
          <p>
            This will permanently remove the feature view. This action cannot be
            undone.
          </p>
        </EuiConfirmModal>
      )}

      {isEditModalOpen && data && (
        <FeatureViewFormModal
          onClose={() => {
            setIsEditModalOpen(false);
            setEditError(null);
          }}
          onSubmit={handleEditSubmit}
          initialData={buildEditFormData(data)}
          isEdit
          isSubmitting={applyFeatureView.isLoading}
          submitError={editError}
        />
      )}
    </EuiPageTemplate>
  );
};

export default RegularFeatureInstance;
