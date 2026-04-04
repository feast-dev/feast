import { useContext } from "react";
import { useParams } from "react-router-dom";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import { useResolvedMode } from "../../queries/useLoadRegistry";
import useResourceQuery, {
  dataSourceDetailPath,
} from "../../queries/useResourceQuery";

const useLoadDataSource = (dataSourceName: string) => {
  const { projectName } = useParams();
  const mode = useResolvedMode();

  const dsQuery = useResourceQuery<any>({
    resourceType: `data-source:${dataSourceName}`,
    project: projectName,
    protoSelect: (d) => ({
      dataSource: d.objects.dataSources?.find(
        (ds: any) => ds.name === dataSourceName,
      ),
      relationships: d.relationships,
    }),
    restPath: dataSourceDetailPath(dataSourceName, projectName || ""),
    restSelect: (d) => ({
      dataSource: d,
      relationships: d?.relationships || [],
    }),
    enabled: !!dataSourceName,
  });

  const dataSource = dsQuery.data?.dataSource;
  const relationships = dsQuery.data?.relationships || [];

  const consumingFeatureViews =
    mode === "proto"
      ? relationships.filter(
          (relationship: any) =>
            relationship.source.type === FEAST_FCO_TYPES.dataSource &&
            relationship.source.name === dataSource?.name &&
            relationship.target.type === FEAST_FCO_TYPES.featureView,
        )
      : relationships.filter(
          (rel: any) =>
            rel?.source?.type === "dataSource" &&
            rel?.target?.type === "featureView",
        );

  return {
    ...dsQuery,
    data: dataSource,
    consumingFeatureViews,
  };
};

export default useLoadDataSource;
