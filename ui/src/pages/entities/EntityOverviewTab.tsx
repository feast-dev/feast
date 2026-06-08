import {
  EuiFlexGroup,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiTitle,
  EuiButtonEmpty,
  EuiCallOut,
} from "@elastic/eui";
import {
  EuiPanel,
  EuiText,
  EuiFlexItem,
  EuiSpacer,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
} from "@elastic/eui";
import React, { useContext, useState } from "react";
import { useParams } from "react-router-dom";
import PermissionsDisplay from "../../components/PermissionsDisplay";
import EntityFormModal, {
  EntityFormData,
} from "../../components/EntityFormModal";
import TagsDisplay from "../../components/TagsDisplay";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { FEAST_FCO_TYPES } from "../../parsers/types";

import useLoadRegistry from "../../queries/useLoadRegistry";
import { getEntityPermissions } from "../../utils/permissionUtils";
import { toDate } from "../../utils/timestamp";
import { feast } from "../../protos";
import FeatureViewEdgesList from "./FeatureViewEdgesList";
import useFeatureViewEdgesByEntity from "./useFeatureViewEdgesByEntity";
import useLoadEntity from "./useLoadEntity";
import { useApplyEntity } from "../../queries/mutations/useEntityMutations";

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

const EntityOverviewTab = () => {
  let { entityName, projectName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl, projectName);

  const eName = entityName === undefined ? "" : entityName;
  const { isLoading, isSuccess, isError, data } = useLoadEntity(eName);
  const isEmpty = data === undefined;

  const fvEdges = useFeatureViewEdgesByEntity();
  const fvEdgesSuccess = fvEdges.isSuccess;
  const fvEdgesData = fvEdges.data;

  const viewTypesForEntity: Record<string, string> | undefined =
    fvEdgesSuccess && fvEdgesData && fvEdgesData[eName]
      ? fvEdgesData[eName].reduce((acc: Record<string, string>, r) => {
          acc[r.target.name] =
            r.target.type === "labelView" ? "labelView" : "featureView";
          return acc;
        }, {})
      : undefined;

  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const applyEntity = useApplyEntity();

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
        setErrorMessage(null);
        setSuccessMessage(`Entity "${formData.name}" updated successfully.`);
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setErrorMessage(message);
        setTimeout(() => setErrorMessage(null), 8000);
      },
    });
  };

  return (
    <React.Fragment>
      {isLoading && (
        <React.Fragment>
          <EuiLoadingSpinner size="m" /> Loading
        </React.Fragment>
      )}
      {isEmpty && <p>No entity with name: {entityName}</p>}
      {isError && <p>Error loading entity: {entityName}</p>}
      {isSuccess && data && (
        <React.Fragment>
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
          {errorMessage && (
            <>
              <EuiCallOut
                title={errorMessage}
                color="danger"
                iconType="alert"
                size="s"
              />
              <EuiSpacer size="m" />
            </>
          )}
          <EuiFlexGroup justifyContent="flexEnd">
            <EuiFlexItem grow={false}>
              <EuiButtonEmpty
                iconType="pencil"
                onClick={() => setIsEditModalOpen(true)}
              >
                Edit Entity
              </EuiButtonEmpty>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiSpacer size="s" />
          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>Properties</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <EuiDescriptionList>
                  <EuiDescriptionListTitle>Join Key</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.spec?.joinKey}
                  </EuiDescriptionListDescription>

                  {data?.spec?.valueType && (
                    <>
                      <EuiDescriptionListTitle>
                        Value Type
                      </EuiDescriptionListTitle>
                      <EuiDescriptionListDescription>
                        {typeof data.spec.valueType === "string"
                          ? data.spec.valueType
                          : String(data.spec.valueType)}
                      </EuiDescriptionListDescription>
                    </>
                  )}

                  <EuiDescriptionListTitle>Description</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.spec?.description || "—"}
                  </EuiDescriptionListDescription>
                </EuiDescriptionList>
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true}>
                <EuiDescriptionList>
                  <EuiDescriptionListTitle>Created</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.meta?.createdTimestamp ? (
                      toDate(data.meta.createdTimestamp).toLocaleDateString(
                        "en-CA",
                      )
                    ) : (
                      <EuiText>
                        No createdTimestamp specified on this entity.
                      </EuiText>
                    )}
                  </EuiDescriptionListDescription>

                  <EuiDescriptionListTitle>Updated</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {data?.meta?.lastUpdatedTimestamp ? (
                      toDate(data.meta.lastUpdatedTimestamp).toLocaleDateString(
                        "en-CA",
                      )
                    ) : (
                      <EuiText>
                        No lastUpdatedTimestamp specified on this entity.
                      </EuiText>
                    )}
                  </EuiDescriptionListDescription>
                </EuiDescriptionList>
              </EuiPanel>
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>Consuming Views</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {fvEdgesSuccess && fvEdgesData ? (
                  fvEdgesData[eName] ? (
                    <FeatureViewEdgesList
                      fvNames={fvEdgesData[eName].map((r) => {
                        return r.target.name;
                      })}
                      viewTypes={viewTypesForEntity}
                    />
                  ) : (
                    <EuiText>No views consume this entity</EuiText>
                  )
                ) : (
                  <EuiText>
                    Error loading views that consume this entity.
                  </EuiText>
                )}
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true} grow={false}>
                <EuiTitle size="xs">
                  <h3>Labels</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {data?.spec?.tags ? (
                  <TagsDisplay tags={data.spec.tags} />
                ) : (
                  <EuiText>No labels specified on this entity.</EuiText>
                )}
              </EuiPanel>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>Permissions</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                {registryQuery.data?.permissions ? (
                  <PermissionsDisplay
                    permissions={getEntityPermissions(
                      registryQuery.data.permissions,
                      FEAST_FCO_TYPES.entity,
                      eName,
                    )}
                  />
                ) : (
                  <EuiText>No permissions defined for this entity.</EuiText>
                )}
              </EuiPanel>
            </EuiFlexItem>
          </EuiFlexGroup>
        </React.Fragment>
      )}

      {isEditModalOpen && data && (
        <EntityFormModal
          onClose={() => setIsEditModalOpen(false)}
          onSubmit={handleEditSubmit}
          initialData={buildEditFormData(data)}
          isEdit
        />
      )}
    </React.Fragment>
  );
};
export default EntityOverviewTab;
