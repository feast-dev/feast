import { useContext } from "react";
import { useParams } from "react-router-dom";
import RegistryPathContext from "../contexts/RegistryPathContext";
import useLoadRegistry from "./useLoadRegistry";

const useLoadRelationshipData = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { projectName } = useParams();
  const registryQuery = useLoadRegistry(registryUrl, projectName);

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
