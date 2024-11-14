import React from "react";
import { render, RenderOptions } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "react-query";
import { QueryParamProvider } from "use-query-params";
import { ReactRouter6Adapter } from 'use-query-params/adapters/react-router-6';
import { MemoryRouter as Router } from "react-router-dom";

interface ProvidersProps {
  children: React.ReactNode;
}

const queryClient = new QueryClient();

const AllTheProviders = ({ children }: ProvidersProps) => {
  return (
    <QueryClientProvider client={queryClient}>
      <Router
        // Disable v7_relativeSplatPath: custom tab routes don't currently work with it
        future={{ v7_relativeSplatPath: false, v7_startTransition: true }}
        initialEntries={["/"]}
      >
        <QueryParamProvider adapter={ReactRouter6Adapter}>
          {children}
        </QueryParamProvider>
      </Router>
    </QueryClientProvider>
  );
};

const customRender = (
  ui: React.ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) => render(ui, { wrapper: AllTheProviders, ...options });

// re-export everything
export * from "@testing-library/react";

// override render method
export { customRender as render };
