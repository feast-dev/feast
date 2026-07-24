import React from "react";

import { setupServer } from "msw/node";
import { render } from "./test-utils";
import {
  waitFor,
  screen,
  waitForElementToBeRemoved,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import FeastUISansProviders from "./FeastUISansProviders";
import { allRestHandlers } from "./mocks/handlers";

import { readFileSync } from "fs";
import { feast } from "./protos";
import path from "path";

// declare which API requests to mock
const server = setupServer(...allRestHandlers);
const registry = readFileSync(path.resolve(__dirname, "../public/registry.db"));
const parsedRegistry = feast.core.Registry.decode(registry);

// establish API mocking before all tests
beforeAll(() => server.listen());
// reset any request handlers that are declared as a part of our tests
// (i.e. for testing one-time error scenarios)
afterEach(() => server.resetHandlers());
// clean up once the tests are done
afterAll(() => server.close());

test("full app rendering", async () => {
  render(<FeastUISansProviders />);

  // Rendering the app without any paths should mean
  // rendering the <RootProjectSelectionPage />
  // Therefore we should expect to see
  // "Welcome to Feast."
  const noProjectSelectedElement = screen.getByText(/Welcome to Feast/i);

  expect(noProjectSelectedElement).toBeInTheDocument();

  // Wait for the Redirect, and check that it got removed
  await waitForElementToBeRemoved(noProjectSelectedElement);

  expect(screen.queryByText(/Welcome to Feast/i)).toBeNull();

  // Explore Panel Should Appear
  expect(screen.getByText(/Explore this Project/i)).toBeInTheDocument();

  // It should load the default project, which is credit_scoring_aws
  // The heading shows the display name from projects-list.json
  await waitFor(() => {
    expect(
      screen.getByRole("heading", { name: /Credit Score Project/i }),
    ).toBeInTheDocument();
  });
});

test("routes are reachable", async () => {
  const user = userEvent.setup();

  render(<FeastUISansProviders />);

  // Wait for content to load
  await screen.findByText(/Explore this Project/i);

  const mainRoutes = [
    { link: "Data Sources", heading: "Data Sources" },
    { link: "Entities", heading: "Entities" },
    { link: "Feature Views", heading: "Feature Views" },
    { link: "Feature Services", heading: "Feature Services" },
    { link: "Datasets", heading: "Data Catalog" },
  ];

  for (const { link, heading } of mainRoutes) {
    // Main heading shouldn't start with the route name
    expect(
      screen.queryByRole("heading", { name: heading, level: 1 }),
    ).toBeNull();

    const routeRegExp = new RegExp(link, "i");

    await user.click(screen.getByRole("link", { name: routeRegExp }));

    // Should land on a page with the heading
    screen.getByRole("heading", {
      name: heading,
      level: 1,
    });
  }
});

const spec = parsedRegistry.featureViews[0].spec!;
const featureViewName = spec.name!;
const featureName = spec.features![0]!.name!;

test("features are reachable", async () => {
  const user = userEvent.setup();

  render(<FeastUISansProviders />);

  // Wait for content to load
  await screen.findByText(/Explore this Project/i);
  const routeRegExp = new RegExp("Feature Views", "i");

  await user.click(screen.getByRole("link", { name: routeRegExp }));

  screen.getByRole("heading", {
    name: "Feature Views",
  });

  await screen.findAllByText(/Feature Views/i);
  const fvRegExp = new RegExp(featureViewName, "i");

  await user.click(screen.getByRole("link", { name: fvRegExp }));

  await screen.findByText(featureName);
  const fRegExp = new RegExp(featureName, "i");

  await user.click(screen.getByRole("link", { name: fRegExp }));
  // Should land on a page with the heading
  // await screen.findByText("Feature: " + featureName);
  screen.getByRole("heading", {
    name: "Feature: " + featureName,
    level: 1,
  });
});
