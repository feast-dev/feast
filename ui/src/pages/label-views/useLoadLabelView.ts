import { useParams } from "react-router-dom";
import useResourceQuery, {
  labelViewDetailPath,
} from "../../queries/useResourceQuery";

const useLoadLabelView = (labelViewName: string) => {
  const { projectName } = useParams();

  return useResourceQuery<any>({
    resourceType: `label-view:${labelViewName}`,
    project: projectName,
    restPath: labelViewDetailPath(labelViewName, projectName || ""),
    restSelect: (d) => d,
    enabled: !!labelViewName,
  });
};

export default useLoadLabelView;
