import { useContext } from "react";
import { useParams } from "react-router-dom";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import useLoadRegistry from "../../queries/useLoadRegistry";

const useLoadEntity = (entityName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const { projectName } = useParams();
  const registryQuery = useLoadRegistry(registryUrl, projectName);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.entities?.find(
          (fv) => fv?.spec?.name === entityName,
        );

  return {
    ...registryQuery,
    data,
  };
};

export default useLoadEntity;
