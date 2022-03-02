import { setupServer } from "msw/node";
import { render } from "../test-utils";
import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import App from "../App";

import {
  projectsListWithDefaultProject,
  creditHistoryRegistry,
  bigQueryProjectRegistry,
} from "../mocks/handlers";

// declare which API requests to mock
const server = setupServer(
  projectsListWithDefaultProject,
  creditHistoryRegistry,
  bigQueryProjectRegistry
);

// establish API mocking before all tests
beforeAll(() => server.listen());
// reset any request handlers that are declared as a part of our tests
// (i.e. for testing one-time error scenarios)
afterEach(() => server.resetHandlers());
// clean up once the tests are done
afterAll(() => server.close());

test("in a full App render, it shows the right initial project", async () => {
  render(<App />);

  const select = await screen.findByRole("combobox", {
    name: "Select a Feast Project",
  });

  // Wait for Project List to Load
  const options = await within(select).findAllByRole("option");

  const topLevelNavigation = await screen.findByRole("navigation", {
    name: "Top Level",
  });

  within(topLevelNavigation).getByDisplayValue("Credit Score Project");

  expect(options.length).toBe(2);

  // Wait for Project Data from Registry to Load
  await screen.findAllByRole("heading", {
    name: /Project:/i,
  });

  // Before User Event: Heading is the credit scoring project
  screen.getByRole("heading", {
    name: /credit_scoring_aws/i,
  });

  // ... and Big Query Project is not selected
  expect(
    within(topLevelNavigation).getByRole("option", {
      name: "Big Query Project",
      selected: false,
    })
  ).toBeInTheDocument();

  // Do the select option user event
  // https://stackoverflow.com/a/69478957
  userEvent.selectOptions(
    // Find the select element
    within(topLevelNavigation).getByRole("combobox"),
    // Find and select the Ireland option
    within(topLevelNavigation).getByRole("option", {
      name: "Big Query Project",
    })
  );

  // The selection should updated
  expect(
    within(topLevelNavigation).getByRole("option", {
      name: "Big Query Project",
      selected: true,
    })
  ).toBeInTheDocument();

  // ... and the new heading should appear
  // meaning we successfully navigated
  await screen.findByRole("heading", {
    name: /dbt_demo/i,
  });
});
