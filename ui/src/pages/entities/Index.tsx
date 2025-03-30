import React, { useContext } from "react";

import { EuiPageTemplate, EuiLoadingSpinner } from "@elastic/eui";

import { EntityIcon } from "../../graphics/EntityIcon";

import useLoadRegistry from "../../queries/useLoadRegistry";
import EntitiesListingTable from "./EntitiesListingTable";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import EntityIndexEmptyState from "./EntityIndexEmptyState";
import ExportButton from "../../components/ExportButton";

const useLoadEntities = () => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.entities;

  return {
    ...registryQuery,
    data,
  };
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
