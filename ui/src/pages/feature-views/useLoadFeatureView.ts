import { useContext } from "react";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";

const useLoadFeatureView = (featureViewName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.mergedFVMap[featureViewName];

  return {
    ...registryQuery,
    data,
  };
};

const useLoadRegularFeatureView = (featureViewName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.featureViews?.find((fv) => {
        return fv?.spec?.name === featureViewName;
      });

  return {
    ...registryQuery,
    data,
  };
};

const useLoadOnDemandFeatureView = (featureViewName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.onDemandFeatureViews?.find((fv) => {
        return fv?.spec?.name === featureViewName;
      });

  return {
    ...registryQuery,
    data,
  };
};

const useLoadStreamFeatureView = (featureViewName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.streamFeatureViews?.find((fv) => {
          return fv.spec?.name === featureViewName;
        });

  return {
    ...registryQuery,
    data,
  };
};

export default useLoadFeatureView;
export { useLoadRegularFeatureView, useLoadOnDemandFeatureView, useLoadStreamFeatureView };
