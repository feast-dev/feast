import React from "react";

import {
  // Feature View Custom Tabs will get these props
  OnDemandFeatureViewCustomTabProps,
} from "../types";

import {
  EuiLoadingContent,
  EuiEmptyPrompt,
  EuiFlexGroup,
  EuiFlexItem,
  EuiCode,
  EuiSpacer,
} from "@elastic/eui";

// Separating out the query is not required,
// but encouraged for code readability
import useDemoQuery from "./useDemoQuery";

const DemoCustomTab = ({
  id,
  feastObjectQuery,
}: OnDemandFeatureViewCustomTabProps) => {
  // Use React Query to fetch data
  // that is custom to this tab.
  // See: https://react-query.tanstack.com/guides/queries
  const { isLoading, isError, isSuccess, data } = useDemoQuery({
    featureView: id,
  });

  if (isLoading) {
    // Handle Loading State
    // https://elastic.github.io/eui/#/display/loading
    return <EuiLoadingContent lines={3} />;
  }

  if (isError) {
    // Handle Data Fetching Error
    // https://elastic.github.io/eui/#/display/empty-prompt
    return (
      <EuiEmptyPrompt
        iconType="alert"
        color="danger"
        title={<h2>Unable to load your demo page</h2>}
        body={
          <p>
            There was an error loading the Dashboard application. Contact your
            administrator for help.
          </p>
        }
      />
    );
  }

  // Feast UI uses the Elastic UI component system.
  // <EuiFlexGroup> and <EuiFlexItem> are particularly
  // useful for layouts.
  return (
    <React.Fragment>
      <EuiFlexGroup>
        <EuiFlexItem grow={1}>
          <p>Hello World. The following is fetched data.</p>
          <EuiSpacer />
          {isSuccess && data && (
            <EuiCode>
              <pre>{JSON.stringify(data, null, 2)}</pre>
            </EuiCode>
          )}
        </EuiFlexItem>
        <EuiFlexItem grow={2}>
          <p>... and this is data from Feast UI&rsquo;s own query.</p>
          <EuiSpacer />
          {feastObjectQuery.isSuccess && feastObjectQuery.data && (
            <EuiCode>
              <pre>{JSON.stringify(feastObjectQuery.data, null, 2)}</pre>
            </EuiCode>
          )}
        </EuiFlexItem>
      </EuiFlexGroup>
    </React.Fragment>
  );
};

export default DemoCustomTab;
