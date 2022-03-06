import React from "react";

import { setupServer } from "msw/node";
import { render } from "./test-utils";
import {
  waitFor,
  screen,
  waitForElementToBeRemoved,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import App from "./App";
import {
  projectsListWithDefaultProject,
  creditHistoryRegistry,
} from "./mocks/handlers";

import registry from "../public/registry.json";

// declare which API requests to mock
const server = setupServer(
  projectsListWithDefaultProject,
  creditHistoryRegistry
);

// establish API mocking before all tests
beforeAll(() => server.listen());
// reset any request handlers that are declared as a part of our tests
// (i.e. for testing one-time error scenarios)
afterEach(() => server.resetHandlers());
// clean up once the tests are done
afterAll(() => server.close());

test("full app rendering", async () => {
  render(<App />);

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

  const projectNameRegExp = new RegExp(registry.project, "i");

  // It should load the default project, which is credit_scoring_aws
  await waitFor(() => {
    expect(screen.getByText(projectNameRegExp)).toBeInTheDocument();
  });
});

const leftClick = { button: 0 };

test("routes are reachable", async () => {
  render(<App />);

  // Wait for content to load
  await screen.findByText(/Explore this Project/i);

  const mainRoutesNames = [
    "Data Sources",
    "Entities",
    "Feature Views",
    "Feature Services",
    "Datasets",
  ];

  for (const routeName of mainRoutesNames) {
    // Main heading shouldn't start with the route name
    expect(
      screen.queryByRole("heading", { name: routeName, level: 1 })
    ).toBeNull();

    const routeRegExp = new RegExp(routeName, "i");

    userEvent.click(
      screen.getByRole("button", { name: routeRegExp }),
      leftClick
    );

    // Should land on a page with the heading
    screen.getByRole("heading", {
      name: routeName,
      level: 1,
    });
  }
});
