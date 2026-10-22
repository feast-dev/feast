import { useParams } from "react-router-dom";
import useResourceQuery, {
  dataSourceDetailPath,
} from "../../queries/useResourceQuery";

const useLoadDataSource = (dataSourceName: string) => {
  const { projectName } = useParams();

  const dsQuery = useResourceQuery<any>({
    resourceType: `data-source:${dataSourceName}`,
    project: projectName,
    restPath: dataSourceDetailPath(dataSourceName, projectName || ""),
    restSelect: (d) => ({
      dataSource: d,
      relationships: d?.relationships || [],
    }),
    enabled: !!dataSourceName,
  });

  const dataSource = dsQuery.data?.dataSource;
  const relationships = dsQuery.data?.relationships || [];

  const consumingFeatureViews = relationships.filter(
    (rel: any) =>
      rel?.source?.type === "dataSource" &&
      (rel?.target?.type === "featureView" ||
        rel?.target?.type === "labelView"),
  );

  return {
    ...dsQuery,
    data: dataSource,
    consumingFeatureViews,
  };
};

export default useLoadDataSource;
