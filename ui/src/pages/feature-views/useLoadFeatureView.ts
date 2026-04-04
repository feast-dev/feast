import { useParams } from "react-router-dom";
import useResourceQuery, {
  featureViewDetailPath,
  restFeatureViewDetailToGeneric,
} from "../../queries/useResourceQuery";
import type { genericFVType } from "../../parsers/mergedFVTypes";

const useLoadFeatureView = (featureViewName: string) => {
  const { projectName } = useParams();

  return useResourceQuery<genericFVType>({
    resourceType: `feature-view:${featureViewName}`,
    project: projectName,
    protoSelect: (d) => d.mergedFVMap[featureViewName],
    restPath: featureViewDetailPath(featureViewName, projectName || ""),
    restSelect: restFeatureViewDetailToGeneric,
    enabled: !!featureViewName,
  });
};

const useLoadRegularFeatureView = (featureViewName: string) => {
  const { projectName } = useParams();

  return useResourceQuery<any>({
    resourceType: `regular-fv:${featureViewName}`,
    project: projectName,
    protoSelect: (d) =>
      d.objects.featureViews?.find(
        (fv: any) => fv?.spec?.name === featureViewName,
      ),
    restPath: featureViewDetailPath(featureViewName, projectName || ""),
    restSelect: (d) => (d?.type === "featureView" ? d : undefined),
    enabled: !!featureViewName,
  });
};

const useLoadOnDemandFeatureView = (featureViewName: string) => {
  const { projectName } = useParams();

  return useResourceQuery<any>({
    resourceType: `odfv:${featureViewName}`,
    project: projectName,
    protoSelect: (d) =>
      d.objects.onDemandFeatureViews?.find(
        (fv: any) => fv?.spec?.name === featureViewName,
      ),
    restPath: featureViewDetailPath(featureViewName, projectName || ""),
    restSelect: (d) => (d?.type === "onDemandFeatureView" ? d : undefined),
    enabled: !!featureViewName,
  });
};

const useLoadStreamFeatureView = (featureViewName: string) => {
  const { projectName } = useParams();

  return useResourceQuery<any>({
    resourceType: `sfv:${featureViewName}`,
    project: projectName,
    protoSelect: (d) =>
      d.objects.streamFeatureViews?.find(
        (fv: any) => fv?.spec?.name === featureViewName,
      ),
    restPath: featureViewDetailPath(featureViewName, projectName || ""),
    restSelect: (d) => (d?.type === "streamFeatureView" ? d : undefined),
    enabled: !!featureViewName,
  });
};

export default useLoadFeatureView;
export {
  useLoadRegularFeatureView,
  useLoadOnDemandFeatureView,
  useLoadStreamFeatureView,
};
