import { useQuery } from "react-query";
import { z } from "zod";

// Use Zod to check the shape of the
// json object being loaded
const demoSchema = z.object({
  hello: z.string(),
  name: z.string().optional(),
});

// Make the type of the object available
type DemoDataType = z.infer<typeof demoSchema>;

interface DemoQueryInterface {
  featureView: string | undefined;
}

const useDemoQuery = ({ featureView }: DemoQueryInterface) => {
  // React Query manages caching for you based on query keys
  // See: https://react-query.tanstack.com/guides/query-keys
  const queryKey = `demo-tab-namespace:${featureView}`;

  // Pass the type to useQuery
  // so that components consuming the
  // result gets nice type hints
  // on the other side.
  return useQuery<DemoDataType>(
    queryKey,
    () => {
      // Customizing the URL based on your needs
      const url = `/demo-custom-tabs/demo.json`;

      return fetch(url)
        .then((res) => res.json())
        .then((data) => demoSchema.parse(data)); // Use zod to parse results
    },
    {
      enabled: !!featureView, // Only start the query when the variable is not undefined
    }
  );
};

export default useDemoQuery;
export type { DemoDataType };