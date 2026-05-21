import { useParams } from "react-router-dom";
import useResourceQuery, {
  entityDetailPath,
} from "../../queries/useResourceQuery";

const useLoadEntity = (entityName: string) => {
  const { projectName } = useParams();

  return useResourceQuery<any>({
    resourceType: `entity:${entityName}`,
    project: projectName,
    restPath: entityDetailPath(entityName, projectName || ""),
    restSelect: (d) => d,
    enabled: !!entityName,
  });
};

export default useLoadEntity;
