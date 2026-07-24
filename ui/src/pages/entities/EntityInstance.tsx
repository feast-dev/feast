import React, { useState } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageTemplate,
  EuiButton,
  EuiButtonEmpty,
  EuiConfirmModal,
} from "@elastic/eui";

import { EntityIcon } from "../../graphics/EntityIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import EntityOverviewTab from "./EntityOverviewTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import EntityFormModal, {
  EntityFormData,
} from "../../components/EntityFormModal";
import {
  useApplyEntity,
  useDeleteEntity,
} from "../../queries/mutations/useEntityMutations";
import useLoadEntity from "./useLoadEntity";
import {
  useEntityCustomTabs,
  useEntityCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";
import { feast } from "../../protos";

const buildEditFormData = (entity: feast.core.IEntity): EntityFormData => {
  const tags = entity.spec?.tags
    ? Object.entries(entity.spec.tags).map(([key, value]) => ({
        key,
        value: String(value),
      }))
    : [];

  const joinKeys = entity.spec?.joinKey ? [entity.spec.joinKey] : [""];

  return {
    name: entity.spec?.name || "",
    description: entity.spec?.description || "",
    joinKeys,
    valueType: String(entity.spec?.valueType ?? 0),
    tags,
  };
};

const EntityInstance = () => {
  const navigate = useNavigate();
  let { entityName, projectName } = useParams();

  const { customNavigationTabs } = useEntityCustomTabs(navigate);
  const CustomTabRoutes = useEntityCustomTabRoutes();

  useDocumentTitle(`${entityName} | Entity | Feast`);

  const { data } = useLoadEntity(entityName || "");
  const applyEntity = useApplyEntity();
  const deleteEntity = useDeleteEntity();

  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  const handleDelete = () => {
    deleteEntity.mutate(
      { name: entityName || "", project: projectName || "" },
      {
        onSuccess: () => {
          navigate(`/p/${projectName}/entity`);
        },
      },
    );
  };

  const handleEditSubmit = (formData: EntityFormData) => {
    const payload = {
      name: formData.name,
      project: projectName || "",
      join_key: formData.joinKeys[0] || formData.name,
      value_type: parseInt(formData.valueType, 10),
      description: formData.description,
      tags: Object.fromEntries(
        formData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
      ),
      owner: "",
    };
    applyEntity.mutate(payload, {
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
        iconType={EntityIcon}
        pageTitle={`Entity: ${entityName}`}
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
          <Route path="/" element={<EntityOverviewTab />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>

      {showDeleteConfirm && (
        <EuiConfirmModal
          title={`Delete "${entityName}"?`}
          onCancel={() => setShowDeleteConfirm(false)}
          onConfirm={handleDelete}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deleteEntity.isLoading}
        >
          <p>
            This will permanently remove the entity. This action cannot be
            undone.
          </p>
        </EuiConfirmModal>
      )}

      {isEditModalOpen && data && (
        <EntityFormModal
          onClose={() => {
            setIsEditModalOpen(false);
            setEditError(null);
          }}
          onSubmit={handleEditSubmit}
          initialData={buildEditFormData(data)}
          isEdit
          isSubmitting={applyEntity.isLoading}
          submitError={editError}
        />
      )}
    </EuiPageTemplate>
  );
};

export default EntityInstance;
