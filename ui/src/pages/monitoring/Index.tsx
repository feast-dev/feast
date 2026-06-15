import React, { useState, useContext, useMemo } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  EuiPageTemplate,
  EuiSpacer,
  EuiTabbedContent,
  EuiTabbedContentTab,
  EuiEmptyPrompt,
  EuiButton,
  EuiCallOut,
} from "@elastic/eui";

import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import {
  useFeatureMetrics,
  useFeatureViewMetrics,
  useFeatureServiceMetrics,
  useComputeMetrics,
} from "../../queries/useMonitoringApi";
import FeatureMetricsTable from "./FeatureMetricsTable";
import FeatureViewMetricsPanel from "./FeatureViewMetricsPanel";
import FeatureServiceMetricsPanel from "./FeatureServiceMetricsPanel";
import MetricsFilters from "./components/MetricsFilters";

const MonitoringIndex = () => {
  useDocumentTitle("Monitoring | Feast");

  const { projectName } = useParams();
  const navigate = useNavigate();
  const registryUrl = useContext(RegistryPathContext);
  const { data: registryData } = useLoadRegistry(registryUrl, projectName);

  const [selectedFV, setSelectedFV] = useState("");
  const [granularity, setGranularity] = useState("baseline");
  const [dataSourceType, setDataSourceType] = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  const isBaseline = granularity === "baseline";

  const handleGranularityChange = (g: string) => {
    setGranularity(g);
    if (g === "baseline") {
      setStartDate("");
      setEndDate("");
    }
  };

  const filters = useMemo(
    () => ({
      project: projectName || "",
      feature_view_name: selectedFV || undefined,
      granularity: isBaseline ? undefined : granularity || undefined,
      data_source_type: dataSourceType || undefined,
      start_date: isBaseline ? undefined : startDate || undefined,
      end_date: isBaseline ? undefined : endDate || undefined,
      is_baseline: isBaseline || undefined,
    }),
    [
      projectName,
      selectedFV,
      granularity,
      isBaseline,
      dataSourceType,
      startDate,
      endDate,
    ],
  );

  const featureQuery = useFeatureMetrics(filters);
  const fvQuery = useFeatureViewMetrics(filters);
  const fsQuery = useFeatureServiceMetrics({
    project: projectName || "",
    granularity: isBaseline ? undefined : granularity || undefined,
    data_source_type: dataSourceType || undefined,
    start_date: startDate || undefined,
    end_date: endDate || undefined,
    is_baseline: isBaseline || undefined,
  });
  const computeMutation = useComputeMetrics();

  const featureViews = useMemo(() => {
    if (!registryData?.mergedFVList) return [];
    return registryData.mergedFVList.map((fv: any) => fv.name as string);
  }, [registryData]);

  const handleFeatureClick = (fvName: string, featureName: string) => {
    navigate(`/p/${projectName}/monitoring/feature/${fvName}/${featureName}`);
  };

  const uniqueFeatureCount = useMemo(() => {
    if (!featureQuery.data) return 0;
    const seen = new Set<string>();
    for (const m of featureQuery.data) {
      seen.add(`${m.feature_view_name}::${m.feature_name}`);
    }
    return seen.size;
  }, [featureQuery.data]);

  const handleRefresh = () => {
    featureQuery.refetch();
    fvQuery.refetch();
    fsQuery.refetch();
  };

  const handleCompute = () => {
    computeMutation.mutate({
      project: projectName || "",
      feature_view_name: selectedFV || undefined,
    });
  };

  const hasError = featureQuery.isError && fvQuery.isError && fsQuery.isError;
  const hasData =
    (featureQuery.data && featureQuery.data.length > 0) ||
    (fvQuery.data && fvQuery.data.length > 0);

  const tabs: EuiTabbedContentTab[] = [
    {
      id: "features",
      name: `Features${uniqueFeatureCount > 0 ? ` (${uniqueFeatureCount})` : ""}`,
      content: (
        <>
          <EuiSpacer size="m" />
          <FeatureMetricsTable
            metrics={featureQuery.data || []}
            isLoading={featureQuery.isLoading}
            onFeatureClick={handleFeatureClick}
          />
        </>
      ),
    },
    {
      id: "feature-views",
      name: "Feature Views",
      content: (
        <>
          <EuiSpacer size="m" />
          <FeatureViewMetricsPanel
            metrics={fvQuery.data || []}
            isLoading={fvQuery.isLoading}
            title="Feature View Data Quality"
            description="Aggregated data quality metrics per feature view — tracks null rates, row counts, and feature health."
          />
        </>
      ),
    },
    {
      id: "feature-services",
      name: "Feature Services",
      content: (
        <>
          <EuiSpacer size="m" />
          <FeatureServiceMetricsPanel
            metrics={fsQuery.data || []}
            isLoading={fsQuery.isLoading}
          />
        </>
      ),
    },
  ];

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType="monitoringApp"
        pageTitle="Monitoring"
        description="Feature quality and data health metrics for your feature store."
        rightSideItems={[
          <EuiButton
            key="compute"
            fill
            size="s"
            iconType="playFilled"
            onClick={handleCompute}
            isLoading={computeMutation.isLoading}
          >
            Compute Metrics
          </EuiButton>,
        ]}
      />
      <EuiPageTemplate.Section>
        {hasError && (
          <>
            <EuiCallOut
              title="Monitoring service unavailable"
              color="warning"
              iconType="alert"
            >
              <p>
                Could not connect to the monitoring API. Make sure the Feast
                registry server is running with monitoring enabled.
              </p>
            </EuiCallOut>
            <EuiSpacer />
          </>
        )}

        <MetricsFilters
          featureViews={featureViews}
          selectedFeatureView={selectedFV}
          onFeatureViewChange={setSelectedFV}
          granularity={granularity}
          onGranularityChange={handleGranularityChange}
          dataSourceType={dataSourceType}
          onDataSourceTypeChange={setDataSourceType}
          startDate={startDate}
          onStartDateChange={setStartDate}
          endDate={endDate}
          onEndDateChange={setEndDate}
          onRefresh={handleRefresh}
          isLoading={featureQuery.isLoading}
          datesDisabled={isBaseline}
        />

        <EuiSpacer />

        {!hasData && !featureQuery.isLoading && !hasError && (
          <EuiEmptyPrompt
            iconType="monitoringApp"
            title={<h2>No Metrics Yet</h2>}
            body={
              <p>
                No monitoring data has been computed for this project. Click
                &quot;Compute Metrics&quot; to run data quality analysis on your
                feature views, or use the CLI:{" "}
                <code>feast monitor run --data-source batch</code>
              </p>
            }
            actions={
              <EuiButton
                fill
                iconType="playFilled"
                onClick={handleCompute}
                isLoading={computeMutation.isLoading}
              >
                Compute Metrics
              </EuiButton>
            }
          />
        )}

        {(hasData || featureQuery.isLoading) && (
          <EuiTabbedContent tabs={tabs} initialSelectedTab={tabs[0]} />
        )}

        {computeMutation.isSuccess && (
          <>
            <EuiSpacer />
            <EuiCallOut
              title="Metrics computed successfully"
              color="success"
              iconType="check"
            >
              <p>
                Data quality metrics have been computed. The table above has
                been refreshed.
              </p>
            </EuiCallOut>
          </>
        )}

        {computeMutation.isError && (
          <>
            <EuiSpacer />
            <EuiCallOut
              title="Failed to compute metrics"
              color="danger"
              iconType="alert"
            >
              <p>{(computeMutation.error as Error)?.message}</p>
            </EuiCallOut>
          </>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default MonitoringIndex;
