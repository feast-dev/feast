import { FEAST_FCO_TYPES } from "../../parsers/types";
import { useParams } from "react-router-dom";
import { EntityReference } from "../../parsers/parseEntityRelationships";
import { useResolvedMode } from "../../queries/useLoadRegistry";
import useResourceQuery, {
  featureServiceDetailPath,
} from "../../queries/useResourceQuery";

const useLoadFeatureService = (featureServiceName: string) => {
  const { projectName } = useParams();
  const mode = useResolvedMode();

  const fsQuery = useResourceQuery<any>({
    resourceType: `feature-service:${featureServiceName}`,
    project: projectName,
    protoSelect: (d) => ({
      featureService: d.objects.featureServices?.find(
        (fs: any) => fs?.spec?.name === featureServiceName,
      ),
      indirectRelationships: d.indirectRelationships,
      permissions: d.permissions,
    }),
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
      : mode === "proto"
        ? indirectRelationships
            .filter(
              (relationship: any) =>
                relationship.target.type ===
                  FEAST_FCO_TYPES.featureService &&
                relationship.target.name === featureService?.spec?.name &&
                relationship.source.type === FEAST_FCO_TYPES.entity,
            )
            .map((relationship: any) => relationship.source)
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
    data: featureService
      ? { ...featureService, permissions }
      : undefined,
    entities,
  };
};

export default useLoadFeatureService;
