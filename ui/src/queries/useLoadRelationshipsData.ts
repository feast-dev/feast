import { useContext } from "react";
import RegistryPathContext from "../contexts/RegistryPathContext";
import useLoadRegistry from "./useLoadRegistry";

const useLoadRelationshipData = () => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.relationships;

  return {
    ...registryQuery,
    data,
  };
};

export default useLoadRelationshipData;
