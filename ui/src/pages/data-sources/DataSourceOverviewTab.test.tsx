import React from "react";
import { EuiProvider } from "@elastic/eui";
import { QueryClient, QueryClientProvider } from "react-query";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { render, screen } from "@testing-library/react";

import DataSourceOverviewTab, {
  formatOneofLabel,
  getConnectionRows,
} from "./DataSourceOverviewTab";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import DataModeContext from "../../contexts/DataModeContext";

const server = setupServer();

beforeAll(() => server.listen());
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("formatOneofLabel", () => {
  test("renders a oneof format object as a display string", () => {
    expect(formatOneofLabel({ parquetFormat: {} })).toBe("parquet");
    expect(formatOneofLabel({ delimitedFormat: { delimiter: "," } })).toBe(
      "delimited (,)",
    );
    expect(formatOneofLabel({ avroFormat: {} })).toBe("avro");
  });

  test("passes through plain strings and handles empty input", () => {
    expect(formatOneofLabel("parquet")).toBe("parquet");
    expect(formatOneofLabel(null)).toBeNull();
    expect(formatOneofLabel(undefined)).toBeNull();
    expect(formatOneofLabel({})).toBeNull();
  });
});

describe("getConnectionRows", () => {
  test("File Format from a REST-shaped parquet oneof is a string, not an object", () => {
    const rows = getConnectionRows({
      fileOptions: { uri: "data/x.parquet", fileFormat: { parquetFormat: {} } },
    });
    const fmt = rows.find((r) => r.title === "File Format");
    expect(fmt).toBeDefined();
    expect(typeof fmt!.value).toBe("string");
    expect(fmt!.value).toBe("parquet");
  });
});

const parquetDataSource = {
  name: "driver_stats",
  type: "BATCH_FILE",
  description: "Driver statistics",
  timestampField: "event_timestamp",
  fileOptions: {
    uri: "data/driver_stats.parquet",
    fileFormat: { parquetFormat: {} },
  },
  meta: {},
  relationships: [],
};

const renderOverview = () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <RegistryPathContext.Provider value="/api/v1">
        <DataModeContext.Provider value={{}}>
          <EuiProvider>
            <MemoryRouter initialEntries={["/p/proj/data-source/driver_stats"]}>
              <Routes>
                <Route
                  path="/p/:projectName/data-source/:dataSourceName"
                  element={<DataSourceOverviewTab />}
                />
              </Routes>
            </MemoryRouter>
          </EuiProvider>
        </DataModeContext.Provider>
      </RegistryPathContext.Provider>
    </QueryClientProvider>,
  );
};

test("renders a Parquet file source from the REST payload without crashing", async () => {
  server.use(
    http.get("/api/v1/data_sources/driver_stats", () =>
      HttpResponse.json(parquetDataSource),
    ),
  );

  renderOverview();

  expect(await screen.findByText("parquet")).toBeInTheDocument();
  expect(screen.getByText("data/driver_stats.parquet")).toBeInTheDocument();
});
