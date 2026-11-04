import { useParams } from "react-router-dom";
import { EntityReference } from "../../parsers/parseEntityRelationships";
import useResourceQuery, {
  featureServiceDetailPath,
} from "../../queries/useResourceQuery";

const useLoadFeatureService = (featureServiceName: string) => {
  const { projectName } = useParams();

  const fsQuery = useResourceQuery<any>({
    resourceType: `feature-service:${featureServiceName}`,
    project: projectName,
    restPath: featureServiceDetailPath(featureServiceName, projectName || ""),
    restSelect: (d) => ({
      featureService: d,
      indirectRelationships: d?.relationships || [],
      permissions: d?.permissions || [],
    }),
    enabled: !!featureServiceName,
  });

  const featureService = fsQuery.data?.featureService;
  const indirectRelationships = fsQuery.data?.indirectRelationships || [];
  const permissions = fsQuery.data?.permissions || [];

  let entities: EntityReference[] | undefined =
    featureService === undefined
      ? undefined
      : indirectRelationships
          .filter(
            (rel: any) =>
              rel?.target?.type === "featureService" &&
              rel?.source?.type === "entity",
          )
          .map((rel: any) => rel.source);

  if (entities) {
    const entityToName: { [key: string]: EntityReference } = {};
    for (const entity of entities) {
      entityToName[entity.name] = entity;
    }
    entities = Object.values(entityToName);
  }

  return {
    ...fsQuery,
    data: featureService ? { ...featureService, permissions } : undefined,
    entities,
  };
};

export default useLoadFeatureService;
