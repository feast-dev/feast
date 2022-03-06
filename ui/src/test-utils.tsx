import React from "react";
import { render } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "react-query";
import { QueryParamProvider } from "use-query-params";
import { MemoryRouter as Router } from "react-router-dom";
import RouteAdapter from "./hacks/RouteAdapter";

interface ProvidersProps {
  children: React.ReactElement;
}

const queryClient = new QueryClient();

const AllTheProviders = ({ children }: ProvidersProps) => {
  return (
    <QueryClientProvider client={queryClient}>
      <Router initialEntries={["/"]}>
        <QueryParamProvider
          ReactRouterRoute={RouteAdapter as unknown as React.FunctionComponent}
        >
          {children}
        </QueryParamProvider>
      </Router>
    </QueryClientProvider>
  );
};

const customRender = (
  ui: React.ReactElement,
  options?: Record<string, unknown>
) => render(ui, { wrapper: AllTheProviders, ...options });

// re-export everything
export * from "@testing-library/react";

// override render method
export { customRender as render };
