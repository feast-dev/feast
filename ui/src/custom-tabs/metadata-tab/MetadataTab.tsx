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
import useLoadRegularFeatureView from "../../pages/feature-views/useLoadFeatureView";
import MetadataQuery from "./MetadataQuery";

const FeatureViewMetadataRow = z.object({
  name: z.string(),
  value: z.string(),
});

type FeatureViewMetadataRowType = z.infer<typeof FeatureViewMetadataRow>;

const LineHeightProp: React.CSSProperties = {
  lineHeight: 1,
}

const EuiFeatureViewMetadataRow = ({name, value}: FeatureViewMetadataRowType) => {
  return (
    <EuiTableRow>
      <EuiTableRowCell>
        {name}
      </EuiTableRowCell>
      <EuiTableRowCell textOnly={false}>
        <EuiCode data-code-language="text">
          <pre style={LineHeightProp}>
            {value}
          </pre>
        </EuiCode>
      </EuiTableRowCell>
    </EuiTableRow>
  );
}

const FeatureViewMetadataTable = (data: any) => {
  var items: FeatureViewMetadataRowType[] = [];

  for (let element in data.data){
    const row: FeatureViewMetadataRowType = {
      name: element,
      value: JSON.stringify(data.data[element], null, 2),
    };
    items.push(row);
    console.log(row);
  }

  return (
    <EuiTable>
      <EuiTableHeader>
        <EuiTableHeaderCell>
          Metadata Item Name
        </EuiTableHeaderCell>
        <EuiTableHeaderCell>
          Metadata Item Value
        </EuiTableHeaderCell>
      </EuiTableHeader>
      {items.map((item) => {
        return <EuiFeatureViewMetadataRow name={item.name} value={item.value} />
      })}
    </EuiTable>
  )
}

const MetadataTab = () => {
  const fName = "credit_history"
  const { isLoading, isError, isSuccess, data } = MetadataQuery(fName);
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
            <FeatureViewMetadataTable data={data} />
          </EuiPanel>
          </EuiFlexItem>
        </EuiFlexGroup>
      </React.Fragment>
      )}
    </React.Fragment>
  );
};

export default MetadataTab;
