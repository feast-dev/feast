import React from "react";
import { EuiProvider } from "@elastic/eui";
import { QueryClient, QueryClientProvider } from "react-query";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { render, screen, fireEvent } from "@testing-library/react";

import ConnectionStatusPanel from "./ConnectionStatusPanel";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import DataModeContext from "../../contexts/DataModeContext";

const server = setupServer();

beforeAll(() => server.listen());
afterEach(() => {
  server.resetHandlers();
  window.localStorage.clear();
});
afterAll(() => server.close());

const wrap = (name: string) => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return (
    <QueryClientProvider client={queryClient}>
      <RegistryPathContext.Provider value="/api/v1">
        <DataModeContext.Provider value={{}}>
          <EuiProvider>
            <ConnectionStatusPanel dataSourceName={name} projectName="proj" />
          </EuiProvider>
        </DataModeContext.Provider>
      </RegistryPathContext.Provider>
    </QueryClientProvider>
  );
};

test("a test result from one source does not leak into another when the component is reused", async () => {
  server.use(
    http.post("/api/v1/data_sources/sourceA/validate", () =>
      HttpResponse.json({
        name: "sourceA",
        project: "proj",
        status: "success",
        message: "Connected to storage",
        testedAt: "2026-01-01T00:00:00Z",
        columns: [{ name: "x", type: "INT64" }],
      }),
    ),
  );

  const { rerender } = render(wrap("sourceA"));

  fireEvent.click(screen.getByText("Test connection"));
  expect(await screen.findByText("Connected")).toBeInTheDocument();

  // Navigate to a different cached source (same component instance / position).
  rerender(wrap("sourceB"));

  // sourceA's live result must not render under sourceB...
  expect(
    await screen.findByText(/Run a test to check connectivity/),
  ).toBeInTheDocument();
  expect(screen.queryByText("Connected")).not.toBeInTheDocument();

  // ...and must not be persisted under sourceB's storage key.
  expect(window.localStorage.getItem("feast:ds-test:proj:sourceB")).toBeNull();
  expect(
    window.localStorage.getItem("feast:ds-test:proj:sourceA"),
  ).not.toBeNull();
});
