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

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadEntities();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  useDocumentTitle(`Entities | Feast`);

  const handleCreateSubmit = (formData: EntityFormData) => {
    // TODO: Wire to REST API when backend write endpoints are available
    console.log("Entity create payload:", formData);
    setIsModalOpen(false);
    setSuccessMessage(
      `Entity "${formData.name}" is ready to be created. Backend integration coming soon.`,
    );
    setTimeout(() => setSuccessMessage(null), 5000);
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={EntityIcon}
        pageTitle="Entities"
        rightSideItems={[
          <EuiButton
            fill
            iconType="plus"
            onClick={() => setIsModalOpen(true)}
            key="create"
          >
            Create Entity
          </EuiButton>,
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
          onClose={() => setIsModalOpen(false)}
          onSubmit={handleCreateSubmit}
        />
      )}
    </EuiPageTemplate>
  );
};

export default Index;
