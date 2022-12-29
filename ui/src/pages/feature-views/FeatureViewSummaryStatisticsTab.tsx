import React from "react";

import { EuiEmptyPrompt, EuiLoadingContent, EuiTitle } from "@elastic/eui";
import { useParams } from "react-router-dom";
import useLoadFeatureViewSummaryStatistics from "../../queries/useLoadFeatureViewSummaryStatistics";
import {
  NumericColumnSummaryStatisticType,
  StringColumnSummaryStatisticType,
} from "../../parsers/featureViewSummaryStatistics";
import NumericFeaturesTable from "../../components/NumericFeaturesTable";

interface ColumnsByGroup {
  INT64?: NumericColumnSummaryStatisticType[];
  STRING?: StringColumnSummaryStatisticType[];
}

const FeatureViewSummaryStatisticsTab = () => {
  let { featureViewName } = useParams();

  if (!featureViewName) {
    throw new Error("Unable to get Feature View Name");
  }

  const { isError, data } =
    useLoadFeatureViewSummaryStatistics(featureViewName);

  if (isError) {
    return (
      <EuiEmptyPrompt
        iconType="alert"
        color="danger"
        title={<h2>Error loading Statistics</h2>}
        body={
          <p>
            There was an error loading statistics for{" "}
            <strong>{featureViewName}</strong>. Please check that statistics
            have been generated.
          </p>
        }
      />
    );
  }

  if (data) {
    const columnsByGroup = Object.entries(
      data.columnsSummaryStatistics
    ).reduce<ColumnsByGroup>((memo, [key, columnStatistics]) => {
      if (columnStatistics.valueType === "INT64") {
        if (!memo["INT64"]) {
          memo[columnStatistics.valueType] = [columnStatistics];
        } else {
          memo["INT64"].push(columnStatistics);
        }
      }

      if (columnStatistics.valueType === "STRING") {
        if (!memo["STRING"]) {
          memo[columnStatistics.valueType] = [columnStatistics];
        } else {
          memo["STRING"].push(columnStatistics);
        }
      }

      return memo;
    }, {});

    return (
      <React.Fragment>
        {columnsByGroup["INT64"] && (
          <>
            <EuiTitle size="xs">
              <h3>Numeric Columns</h3>
            </EuiTitle>
            <NumericFeaturesTable data={columnsByGroup["INT64"]} />
          </>
        )}
      </React.Fragment>
    );
  }

  return <EuiLoadingContent lines={4} />;
};

export default FeatureViewSummaryStatisticsTab;
