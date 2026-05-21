import React from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiSelect,
  EuiFieldText,
  EuiFormRow,
  EuiButton,
} from "@elastic/eui";

interface MetricsFiltersProps {
  featureViews: string[];
  selectedFeatureView: string;
  onFeatureViewChange: (fv: string) => void;
  granularity: string;
  onGranularityChange: (g: string) => void;
  dataSourceType: string;
  onDataSourceTypeChange: (ds: string) => void;
  startDate: string;
  onStartDateChange: (d: string) => void;
  endDate: string;
  onEndDateChange: (d: string) => void;
  onRefresh: () => void;
  isLoading?: boolean;
}

const GRANULARITY_OPTIONS = [
  { value: "", text: "All" },
  { value: "daily", text: "Daily" },
  { value: "weekly", text: "Weekly" },
  { value: "biweekly", text: "Biweekly" },
  { value: "monthly", text: "Monthly" },
  { value: "quarterly", text: "Quarterly" },
];

const DATA_SOURCE_OPTIONS = [
  { value: "", text: "All Sources" },
  { value: "batch", text: "Batch" },
  { value: "log", text: "Log" },
];

const MetricsFilters = ({
  featureViews,
  selectedFeatureView,
  onFeatureViewChange,
  granularity,
  onGranularityChange,
  dataSourceType,
  onDataSourceTypeChange,
  startDate,
  onStartDateChange,
  endDate,
  onEndDateChange,
  onRefresh,
  isLoading,
}: MetricsFiltersProps) => {
  const fvOptions = [
    { value: "", text: "All Feature Views" },
    ...featureViews.map((fv) => ({ value: fv, text: fv })),
  ];

  return (
    <EuiFlexGroup gutterSize="m" alignItems="flexEnd" wrap>
      <EuiFlexItem grow={2}>
        <EuiFormRow label="Feature View">
          <EuiSelect
            options={fvOptions}
            value={selectedFeatureView}
            onChange={(e) => onFeatureViewChange(e.target.value)}
            compressed
          />
        </EuiFormRow>
      </EuiFlexItem>
      <EuiFlexItem grow={1}>
        <EuiFormRow label="Granularity">
          <EuiSelect
            options={GRANULARITY_OPTIONS}
            value={granularity}
            onChange={(e) => onGranularityChange(e.target.value)}
            compressed
          />
        </EuiFormRow>
      </EuiFlexItem>
      <EuiFlexItem grow={1}>
        <EuiFormRow label="Source">
          <EuiSelect
            options={DATA_SOURCE_OPTIONS}
            value={dataSourceType}
            onChange={(e) => onDataSourceTypeChange(e.target.value)}
            compressed
          />
        </EuiFormRow>
      </EuiFlexItem>
      <EuiFlexItem grow={1}>
        <EuiFormRow label="Start Date">
          <EuiFieldText
            type="date"
            value={startDate}
            onChange={(e) => onStartDateChange(e.target.value)}
            compressed
          />
        </EuiFormRow>
      </EuiFlexItem>
      <EuiFlexItem grow={1}>
        <EuiFormRow label="End Date">
          <EuiFieldText
            type="date"
            value={endDate}
            onChange={(e) => onEndDateChange(e.target.value)}
            compressed
          />
        </EuiFormRow>
      </EuiFlexItem>
      <EuiFlexItem grow={false}>
        <EuiButton
          size="s"
          iconType="refresh"
          onClick={onRefresh}
          isLoading={isLoading}
        >
          Refresh
        </EuiButton>
      </EuiFlexItem>
    </EuiFlexGroup>
  );
};

export default MetricsFilters;
