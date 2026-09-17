import React from "react";
import { EuiProvider } from "@elastic/eui";
import { QueryClient, QueryClientProvider } from "react-query";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";

import { render, screen, waitFor } from "@testing-library/react";
import DataModeContext from "../contexts/DataModeContext";
import FeastVersion from "./FeastVersion";

const server = setupServer();

beforeAll(() => server.listen());
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const renderVersion = () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <DataModeContext.Provider value={{}}>
        <EuiProvider>
          <FeastVersion registryPath="/api/v1" />
        </EuiProvider>
      </DataModeContext.Provider>
    </QueryClientProvider>,
  );
};

test("renders the running Feast version", async () => {
  server.use(
    http.get("/api/v1/version", () => HttpResponse.json({ version: "1.2.3" })),
  );

  renderVersion();

  expect(await screen.findByText("Feast v1.2.3")).toBeInTheDocument();
});

test("renders an explicit unknown runtime version", async () => {
  server.use(
    http.get("/api/v1/version", () =>
      HttpResponse.json({ version: "unknown" }),
    ),
  );

  renderVersion();

  expect(await screen.findByText("Feast version unknown")).toBeInTheDocument();
});

test("hides the version when the endpoint is unavailable", async () => {
  let requestHandled = false;
  server.use(
    http.get("/api/v1/version", () => {
      requestHandled = true;
      return HttpResponse.json({ detail: "Not found" }, { status: 404 });
    }),
  );

  const { container } = renderVersion();

  await waitFor(() => expect(requestHandled).toBe(true));
  expect(container).not.toHaveTextContent(/Feast v|Feast version unknown/);
});

test("hides a malformed version response", async () => {
  let requestHandled = false;
  server.use(
    http.get("/api/v1/version", () => {
      requestHandled = true;
      return HttpResponse.json({ feast_version: "1.2.3" });
    }),
  );

  const { container } = renderVersion();

  await waitFor(() => expect(requestHandled).toBe(true));
  expect(container).not.toHaveTextContent(/Feast v|Feast version unknown/);
});
