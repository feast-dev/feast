import React, { useState } from "react";
import { useParams } from "react-router-dom";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiButton,
  EuiCallOut,
  EuiSpacer,
} from "@elastic/eui";

import { EntityIcon } from "../../graphics/EntityIcon";

import EntitiesListingTable from "./EntitiesListingTable";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import EntityIndexEmptyState from "./EntityIndexEmptyState";
import ExportButton from "../../components/ExportButton";
import EntityFormModal, {
  EntityFormData,
} from "../../components/EntityFormModal";
import { useApplyEntity } from "../../queries/mutations/useEntityMutations";
import { useUIVersion } from "../../contexts/UIVersionContext";
import useResourceQuery, {
  entityListPath,
} from "../../queries/useResourceQuery";

const useLoadEntities = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "entities-list",
    project: projectName,
    restPath: entityListPath(projectName),
    restSelect: (d) => d.entities,
  });
};

const formDataToPayload = (formData: EntityFormData, project: string) => ({
  name: formData.name,
  project,
  join_key: formData.joinKeys[0] || formData.name,
  value_type: parseInt(formData.valueType, 10),
  description: formData.description,
  tags: Object.fromEntries(
    formData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
  ),
  owner: "",
});

const Index = () => {
  const { projectName } = useParams();
  const { isV2 } = useUIVersion();

  const { isLoading, isSuccess, isError, data: queryData } =
    useLoadEntitiesREST(projectName || "");
  const data = queryData?.entities;

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const applyEntity = useApplyEntity();

  useDocumentTitle(`Entities | Feast`);

  const handleCreateSubmit = (formData: EntityFormData) => {
    const payload = formDataToPayload(formData, projectName || "");
    applyEntity.mutate(payload, {
      onSuccess: () => {
        setIsModalOpen(false);
        setErrorMessage(null);
        setSuccessMessage(`Entity "${formData.name}" created successfully.`);
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
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={EntityIcon}
        pageTitle="Entities"
        rightSideItems={[
          ...(isV2
            ? [
                <EuiButton
                  fill
                  iconType="plus"
                  onClick={() => setIsModalOpen(true)}
                  key="create"
                >
                  Create Entity
                </EuiButton>,
              ]
            : []),
          <ExportButton
            data={data ?? []}
            fileName="entities"
            formats={["json"]}
            key="export"
          />,
        ]}
      />
      <EuiPageTemplate.Section>
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
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && !data && <EntityIndexEmptyState />}
        {isSuccess && data && <EntitiesListingTable entities={data} />}
      </EuiPageTemplate.Section>

      {isModalOpen && (
        <EntityFormModal
          onClose={() => setIsModalOpen(false)}
          onSubmit={handleCreateSubmit}
        />
      )}
    </EuiPageTemplate>
  );
};

export default Index;
