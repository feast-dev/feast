import React from "react";

import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "react-query";
import { QueryParamProvider } from "use-query-params";
import FeastUISansProviders, { FeastUIConfigs } from "./FeastUISansProviders";

interface FeastUIProps {
  reactQueryClient?: QueryClient;
  feastUIConfigs?: FeastUIConfigs;
}

const defaultQueryClient = new QueryClient();

const FeastUI = ({ reactQueryClient, feastUIConfigs }: FeastUIProps) => {
  const queryClient = reactQueryClient || defaultQueryClient;

  return (
    <BrowserRouter>
      <QueryClientProvider client={queryClient}>
        <QueryParamProvider>
          <FeastUISansProviders feastUIConfigs={feastUIConfigs} />
        </QueryParamProvider>
      </QueryClientProvider>
    </BrowserRouter>
  );
};

export default FeastUI;
export type { FeastUIConfigs };
