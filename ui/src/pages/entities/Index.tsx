import React from "react";
import { useParams } from "react-router-dom";

import { EuiPageTemplate, EuiLoadingSpinner } from "@elastic/eui";

import { EntityIcon } from "../../graphics/EntityIcon";

import EntitiesListingTable from "./EntitiesListingTable";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import EntityIndexEmptyState from "./EntityIndexEmptyState";
import ExportButton from "../../components/ExportButton";
import useResourceQuery, {
  entityListPath,
} from "../../queries/useResourceQuery";

const useLoadEntities = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "entities-list",
    project: projectName,
    protoSelect: (d) => d.objects.entities,
    restPath: entityListPath(projectName),
    restSelect: (d) => d.entities,
  });
};

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadEntities();

  useDocumentTitle(`Entities | Feast`);

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={EntityIcon}
        pageTitle="Entities"
        rightSideItems={[
          <ExportButton
            data={data ?? []}
            fileName="entities"
            formats={["json"]}
          />,
        ]}
      />
      <EuiPageTemplate.Section>
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && !data && <EntityIndexEmptyState />}
        {isSuccess && data && <EntitiesListingTable entities={data} />}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default Index;
