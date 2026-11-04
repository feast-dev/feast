import React from "react";
import {
  EuiDescriptionList,
  EuiDescriptionListDescription,
  EuiDescriptionListTitle,
  EuiFlexGroup,
  EuiFlexItem,
} from "@elastic/eui";
import { CopyBlock, atomOneDark } from "react-code-blocks";
import { feast } from "../../protos";
import { toDate } from "../../utils/timestamp";

interface BatchSourcePropertiesViewProps {
  batchSource: feast.core.IDataSource;
}

const BatchSourcePropertiesView = (props: BatchSourcePropertiesViewProps) => {
  const batchSource = props.batchSource;
  return (
    <React.Fragment>
      <EuiFlexGroup>
        <EuiFlexItem grow={false}>
          <EuiDescriptionList>
            {(batchSource.dataSourceClassType || batchSource.type) && (
              <React.Fragment>
                <EuiDescriptionListTitle>Source Type</EuiDescriptionListTitle>
                {batchSource.dataSourceClassType ? (
                  <EuiDescriptionListDescription>
                    {batchSource.dataSourceClassType.split(".").at(-1)}
                  </EuiDescriptionListDescription>
                ) : feast.core.DataSource.SourceType[batchSource.type!] ? (
                  <EuiDescriptionListDescription>
                    {batchSource.type}
                  </EuiDescriptionListDescription>
                ) : (
                  ""
                )}
              </React.Fragment>
            )}

            {batchSource.owner && (
              <React.Fragment>
                <EuiDescriptionListTitle>Owner</EuiDescriptionListTitle>
                <EuiDescriptionListDescription>
                  {batchSource.owner}
                </EuiDescriptionListDescription>
              </React.Fragment>
            )}
            {batchSource.description && (
              <React.Fragment>
                <EuiDescriptionListTitle>Description</EuiDescriptionListTitle>
                <EuiDescriptionListDescription>
                  {batchSource.description}
                </EuiDescriptionListDescription>
              </React.Fragment>
            )}
            {batchSource.fileOptions && (
              <React.Fragment>
                <EuiDescriptionListTitle>File URL</EuiDescriptionListTitle>
                <EuiDescriptionListDescription>
                  {batchSource.fileOptions ? batchSource.fileOptions.uri : ""}
                </EuiDescriptionListDescription>
              </React.Fragment>
            )}
            {batchSource.bigqueryOptions && (
              <React.Fragment>
                <EuiDescriptionListTitle>
                  Source {batchSource.bigqueryOptions.table ? "Table" : "Query"}
                </EuiDescriptionListTitle>
                {batchSource.bigqueryOptions.table ? (
                  <EuiDescriptionListDescription>
                    {batchSource.bigqueryOptions.table}
                  </EuiDescriptionListDescription>
                ) : (
                  <CopyBlock
                    text={batchSource.bigqueryOptions.query ?? ""}
                    language="sql"
                    showLineNumbers={false}
                    theme={atomOneDark}
                    wrapLongLines
                  />
                )}
              </React.Fragment>
            )}
            {batchSource.meta?.latestEventTimestamp && (
              <React.Fragment>
                <EuiDescriptionListTitle>Latest Event</EuiDescriptionListTitle>
                <EuiDescriptionListDescription>
                  {toDate(
                    batchSource.meta.latestEventTimestamp,
                  ).toLocaleDateString("en-CA")}
                </EuiDescriptionListDescription>
              </React.Fragment>
            )}
            {batchSource.meta?.earliestEventTimestamp && (
              <React.Fragment>
                <EuiDescriptionListTitle>
                  Earliest Event
                </EuiDescriptionListTitle>
                <EuiDescriptionListDescription>
                  {toDate(
                    batchSource.meta?.earliestEventTimestamp,
                  ).toLocaleDateString("en-CA")}
                </EuiDescriptionListDescription>
              </React.Fragment>
            )}
          </EuiDescriptionList>
        </EuiFlexItem>
      </EuiFlexGroup>
    </React.Fragment>
  );
};

export default BatchSourcePropertiesView;
