import { useContext } from "react";
import { useParams } from "react-router-dom";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import useLoadRegistry from "../../queries/useLoadRegistry";

const useLoadDataSource = (dataSourceName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const { projectName } = useParams();
  const registryQuery = useLoadRegistry(registryUrl, projectName);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.dataSources?.find(
          (ds) => ds.name === dataSourceName,
        );

  const consumingFeatureViews =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.relationships.filter((relationship) => {
          return (
            relationship.source.type === FEAST_FCO_TYPES.dataSource &&
            relationship.source.name === data?.name &&
            relationship.target.type === FEAST_FCO_TYPES.featureView
          );
        });

  return {
    ...registryQuery,
    data,
    consumingFeatureViews,
  };
};

export default useLoadDataSource;
