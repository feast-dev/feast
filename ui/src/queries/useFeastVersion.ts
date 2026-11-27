import { useQuery } from "react-query";

import { useDataMode } from "../contexts/DataModeContext";
import restFetch from "./restApiClient";

interface FeastVersionResponse {
  version: string;
}

const useFeastVersion = (registryPath: string) => {
  const { fetchOptions } = useDataMode();

  return useQuery<FeastVersionResponse, Error>(
    ["feast-version", registryPath],
    () =>
      restFetch<FeastVersionResponse>(registryPath, "/version", fetchOptions),
    {
      enabled: !!registryPath,
      retry: false,
      staleTime: Infinity,
    },
  );
};

export default useFeastVersion;
export type { FeastVersionResponse };
