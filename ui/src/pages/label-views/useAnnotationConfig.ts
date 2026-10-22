import { useContext } from "react";
import { useQuery } from "react-query";
import RegistryPathContext from "../../contexts/RegistryPathContext";

export interface AnnotationConfig {
  label_view: string;
  profile: string;
  field_roles: Record<string, string>;
  label_values: Record<string, string[]>;
  label_widgets: Record<string, string>;
  entities: string[];
  features: string[];
  labeler_field: string;
  push_source_name: string | null;
}

const useAnnotationConfig = (labelViewName: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const baseUrl = registryUrl?.replace(/\/$/, "") || "/api/v1";

  return useQuery<AnnotationConfig>(
    ["annotation-config", labelViewName, registryUrl],
    async () => {
      const response = await fetch(
        `${baseUrl}/annotation-config/${encodeURIComponent(labelViewName)}`,
      );
      if (!response.ok) {
        throw new Error(
          `Failed to load annotation config (${response.status})`,
        );
      }
      return response.json();
    },
    {
      enabled: !!labelViewName && !!registryUrl,
      staleTime: 60_000,
    },
  );
};

export default useAnnotationConfig;
