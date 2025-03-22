import { useContext } from "react";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";

const useLoadFeature = (featureViewName: string, featureName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.featureViews?.find((fv) => {
          return fv?.spec?.name === featureViewName;
        });

  const featureData =
    data === undefined
      ? undefined
      : data?.spec?.features?.find((f) => {
          return f.name === featureName;
        });

  return {
    ...registryQuery,
    featureData,
  };
};

export default useLoadFeature;
