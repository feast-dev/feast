import { useParams } from "react-router-dom";
import useResourceQuery, {
  featureDetailPath,
} from "../../queries/useResourceQuery";

const useLoadFeature = (featureViewName: string, featureName: string) => {
  const { projectName } = useParams();

  const fvQuery = useResourceQuery<any>({
    resourceType: `feature:${featureViewName}:${featureName}`,
    project: projectName,
    protoSelect: (d) =>
      d.objects.featureViews?.find(
        (fv: any) => fv?.spec?.name === featureViewName,
      ),
    restPath: featureDetailPath(
      featureViewName,
      featureName,
      projectName || "",
    ),
    restSelect: (d) => d,
    enabled: !!featureViewName && !!featureName,
  });

  const featureData =
    fvQuery.data === undefined
      ? undefined
      : fvQuery.data?.spec?.features?.find(
          (f: any) => f.name === featureName,
        ) || fvQuery.data;

  return {
    ...fvQuery,
    featureData,
  };
};

export default useLoadFeature;
