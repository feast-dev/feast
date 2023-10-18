import React from "react";
import { z } from "zod";
import {
  EuiCode,
  EuiFlexGroup,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiTable,
  EuiTitle,
  EuiTableHeader,
  EuiTableHeaderCell,
  EuiPanel,
  EuiFlexItem,
  EuiTableRow,
  EuiTableRowCell,
} from "@elastic/eui";
import DataQuery from "./DataQuery";

const FeatureViewDataRow = z.object({
  name: z.string(),
  value: z.string(),
});

type FeatureViewDataRowType = z.infer<typeof FeatureViewDataRow>;

const LineHeightProp: React.CSSProperties = {
  lineHeight: 1,
};

const EuiFeatureViewDataRow = ({ name, value }: FeatureViewDataRowType) => {
  return (
    <EuiTableRow>
      <EuiTableRowCell>{name}</EuiTableRowCell>
      <EuiTableRowCell textOnly={false}>
        <EuiCode data-code-language="text">
          <pre style={LineHeightProp}>{value}</pre>
        </EuiCode>
      </EuiTableRowCell>
    </EuiTableRow>
  );
};

const FeatureViewDataTable = (data: any) => {
  var items: FeatureViewDataRowType[] = [];

  for (let element in data.data) {
    const row: FeatureViewDataRowType = {
      name: element,
      value: JSON.stringify(data.data[element], null, 2),
    };
    items.push(row);
    console.log(row);
  }

  return (
    <EuiTable>
      <EuiTableHeader>
        <EuiTableHeaderCell>Data Item Name</EuiTableHeaderCell>
        <EuiTableHeaderCell>Data Item Value</EuiTableHeaderCell>
      </EuiTableHeader>
      {items.map((item) => {
        return <EuiFeatureViewDataRow name={item.name} value={item.value} />;
      })}
    </EuiTable>
  );
};

const DataTab = () => {
  const fName = "credit_history";
  const { isLoading, isError, isSuccess, data } = DataQuery(fName);
  const isEmpty = data === undefined;

  return (
    <React.Fragment>
      {isLoading && (
        <React.Fragment>
          <EuiLoadingSpinner size="m" /> Loading
        </React.Fragment>
      )}
      {isEmpty && <p>No feature view with name: {fName}</p>}
      {isError && <p>Error loading feature view: {fName}</p>}
      {isSuccess && data && (
        <React.Fragment>
          <EuiFlexGroup>
            <EuiFlexItem>
              <EuiPanel hasBorder={true}>
                <EuiTitle size="xs">
                  <h3>Properties</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <FeatureViewDataTable data={data} />
              </EuiPanel>
            </EuiFlexItem>
          </EuiFlexGroup>
        </React.Fragment>
      )}
    </React.Fragment>
  );
};

export default DataTab;
