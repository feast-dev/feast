import React, { useContext } from "react";

import {
  EuiPageHeader,
  EuiPageContent,
  EuiPageContentBody,
  EuiLoadingSpinner,
} from "@elastic/eui";

import { EntityIcon32 } from "../../graphics/EntityIcon";

import useLoadRegistry from "../../queries/useLoadRegistry";
import EntitiesListingTable from "./EntitiesListingTable";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import EntityIndexEmptyState from "./EntityIndexEmptyState";

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
    <React.Fragment>
      <EuiPageHeader
        restrictWidth
        iconType={EntityIcon32}
        pageTitle="Entities"
      />
      <EuiPageContent
        hasBorder={false}
        hasShadow={false}
        paddingSize="none"
        color="transparent"
        borderRadius="none"
      >
        <EuiPageContentBody>
          {isLoading && (
            <p>
              <EuiLoadingSpinner size="m" /> Loading
            </p>
          )}
          {isError && <p>We encountered an error while loading.</p>}
          {isSuccess && !data && <EntityIndexEmptyState />}
          {isSuccess && data && <EntitiesListingTable entities={data} />}
        </EuiPageContentBody>
      </EuiPageContent>
    </React.Fragment>
  );
};

export default Index;
