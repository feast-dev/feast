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
  const { isLoading, isSuccess, isError, data } = useLoadEntities();
  const isAllProjects = projectName === "all";

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [submitErrorMessage, setSubmitErrorMessage] = useState<string | null>(
    null,
  );
  const applyEntity = useApplyEntity();

  useDocumentTitle(`Entities | Feast`);

  const handleCreateSubmit = (formData: EntityFormData) => {
    setSubmitErrorMessage(null);
    const payload = formDataToPayload(formData, projectName || "");
    applyEntity.mutate(payload, {
      onSuccess: () => {
        setIsModalOpen(false);
        setSubmitErrorMessage(null);
        setSuccessMessage(`Entity "${formData.name}" created successfully.`);
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setSubmitErrorMessage(message);
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
          ...(isAllProjects
            ? []
            : [
                <EuiButton
                  fill
                  iconType="plus"
                  onClick={() => setIsModalOpen(true)}
                  key="create"
                >
                  Create Entity
                </EuiButton>,
              ]),
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
          onClose={() => {
            setIsModalOpen(false);
            setSubmitErrorMessage(null);
          }}
          onSubmit={handleCreateSubmit}
          isSubmitting={applyEntity.isLoading}
          submitError={submitErrorMessage}
        />
      )}
    </EuiPageTemplate>
  );
};

export default Index;
