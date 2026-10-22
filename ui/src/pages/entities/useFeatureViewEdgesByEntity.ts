import useLoadRelationshipData from "../../queries/useLoadRelationshipsData";
import { EntityRelation } from "../../parsers/parseEntityRelationships";

const entityGroupByName = (data: EntityRelation[]) => {
  return data
    .filter((edge) => {
      return edge.source.type === "entity";
    })
    .reduce((memo: Record<string, EntityRelation[]>, current) => {
      if (memo[current.source.name]) {
        memo[current.source.name].push(current);
      } else {
        memo[current.source.name] = [current];
      }

      return memo;
    }, {});
};

const useFeatureViewEdgesByEntity = () => {
  const query = useLoadRelationshipData();

  return {
    ...query,
    data:
      query.isSuccess && query.data ? entityGroupByName(query.data) : undefined,
  };
};

export default useFeatureViewEdgesByEntity;
