import { useQuery } from "react-query";

interface MetadataQueryInterface {
  featureView: string | undefined;
}

const MetadataQuery = (featureView: string) => {
  const queryKey = `metadata-tab-namespace:${featureView}`;

  return useQuery<any>(
    queryKey,
    () => {
      // Customizing the URL based on your needs
      const url = `/demo-custom-tabs/demo.json`;

      return fetch(url)
        .then((res) => res.json())
    },
    {
      enabled: !!featureView, // Only start the query when the variable is not undefined
    }
  );
};

export default MetadataQuery;
