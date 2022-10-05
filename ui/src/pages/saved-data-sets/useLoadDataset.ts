import { useContext } from "react";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";

const useLoadEntity = (entityName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.savedDatasets?.find(
        (fv) => fv.spec?.name === entityName
      );

  return {
    ...registryQuery,
    data,
  };
};

export default useLoadEntity;
