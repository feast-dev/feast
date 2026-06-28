import React, { useMemo, useState } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiFlexGroup,
  EuiFlexItem,
  EuiStat,
  EuiSpacer,
  EuiPanel,
  EuiHorizontalRule,
  EuiTitle,
  EuiText,
  EuiBadge,
  EuiBasicTable,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
  EuiHealth,
  EuiFilterGroup,
  EuiFilterButton,
} from "@elastic/eui";

import { ComputeEngineIcon } from "../../graphics/ComputeEngineIcon";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import {
  useLoadComputeEngine,
  FeatureViewEngineInfo,
} from "../../queries/useLoadComputeEngine";
import EuiCustomLink from "../../components/EuiCustomLink";

interface MaterializationJobRow {
  id: string;
  featureView: string;
  status: "SUCCEEDED" | "RUNNING" | "ERROR" | "WAITING";
  rangeStart: string;
  rangeEnd: string;
  sortKey: number;
}

function formatRange(iso: string | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

function buildJobsFromFeatureViews(
  featureViewInfos: FeatureViewEngineInfo[],
): MaterializationJobRow[] {
  const jobs: MaterializationJobRow[] = [];

  featureViewInfos.forEach((fv) => {
    if (fv.materializationIntervals && fv.materializationIntervals.length > 0) {
      fv.materializationIntervals.forEach((interval, idx) => {
        const startTime =
          (interval as any).startTime || (interval as any).start_time;
        const endTime = (interval as any).endTime || (interval as any).end_time;

        const sortMs = endTime
          ? new Date(endTime).getTime()
          : startTime
            ? new Date(startTime).getTime()
            : 0;

        jobs.push({
          id: `${fv.name}-${idx}`,
          featureView: fv.name,
          status: "SUCCEEDED",
          rangeStart: formatRange(startTime),
          rangeEnd: formatRange(endTime),
          sortKey: sortMs,
        });
      });
    }
  });

  return jobs.sort((a, b) => {
    try {
      return b.sortKey - a.sortKey;
    } catch {
      return 0;
    }
  });
}

const statusColorMap: Record<string, string> = {
  SUCCEEDED: "success",
  RUNNING: "primary",
  ERROR: "danger",
  WAITING: "subdued",
};

const OverviewTab = ({
  engineInfo,
  featureViewInfos,
  projectName,
}: {
  engineInfo: any;
  featureViewInfos: FeatureViewEngineInfo[];
  projectName: string | undefined;
}) => {
  const materializedCount = featureViewInfos.filter(
    (fv) => fv.lastMaterialized,
  ).length;
  const overrideCount = featureViewInfos.filter((fv) => fv.hasOverride).length;

  const fvColumns = [
    {
      name: "Feature View",
      field: "name",
      sortable: true,
      render: (name: string) => (
        <EuiCustomLink to={`/p/${projectName}/feature-view/${name}`}>
          {name}
        </EuiCustomLink>
      ),
    },
    {
      name: "Type",
      field: "type",
      sortable: true,
      render: (type: string) => <EuiBadge color="hollow">{type}</EuiBadge>,
    },
    {
      name: "Online",
      field: "online",
      render: (online: boolean) => (
        <EuiHealth color={online ? "success" : "subdued"}>
          {online ? "Yes" : "No"}
        </EuiHealth>
      ),
    },
    {
      name: "Last Materialized",
      field: "lastMaterialized",
      render: (val: string | undefined) => {
        if (!val)
          return (
            <EuiText size="s" color="subdued">
              Never
            </EuiText>
          );
        try {
          return new Date(val).toLocaleString();
        } catch {
          return val;
        }
      },
    },
    {
      name: "Engine Override",
      field: "hasOverride",
      render: (hasOverride: boolean, item: any) => {
        if (!hasOverride) {
          return (
            <EuiText size="s" color="subdued">
              None
            </EuiText>
          );
        }
        const keys = item.overrides ? Object.keys(item.overrides) : [];
        return (
          <EuiBadge color="primary">
            {keys.length > 0 ? keys.join(", ") : "Custom"}
          </EuiBadge>
        );
      },
    },
  ];

  return (
    <React.Fragment>
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiStat
            title={engineInfo?.engineType || "local"}
            description="Engine Type"
            titleSize="s"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={String(featureViewInfos.length)}
            description="Feature Views"
            titleSize="s"
            titleColor="primary"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={String(materializedCount)}
            description="Materialized"
            titleSize="s"
            titleColor="success"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={String(overrideCount)}
            description="With Overrides"
            titleSize="s"
          />
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="l" />

      <EuiPanel hasBorder={true} hasShadow={false}>
        <EuiTitle size="xs">
          <h3>Engine Configuration</h3>
        </EuiTitle>
        <EuiHorizontalRule margin="xs" />
        <EuiDescriptionList>
          <EuiDescriptionListTitle>Type</EuiDescriptionListTitle>
          <EuiDescriptionListDescription>
            <EuiBadge color="primary">
              {engineInfo?.engineType || "local"}
            </EuiBadge>
          </EuiDescriptionListDescription>

          <EuiDescriptionListTitle>Class</EuiDescriptionListTitle>
          <EuiDescriptionListDescription>
            {engineInfo?.engineClass || "LocalComputeEngine"}
          </EuiDescriptionListDescription>
        </EuiDescriptionList>

        {engineInfo?.config &&
          Object.entries(engineInfo.config).filter(
            ([key, value]) => key !== "type" && value != null && value !== "",
          ).length > 0 && (
            <>
              <EuiSpacer size="m" />
              <EuiTitle size="xxs">
                <h4>Parameters</h4>
              </EuiTitle>
              <EuiHorizontalRule margin="xs" />
              <EuiDescriptionList>
                {Object.entries(engineInfo.config)
                  .filter(
                    ([key, value]) =>
                      key !== "type" && value != null && value !== "",
                  )
                  .map(([key, value]) => (
                    <React.Fragment key={key}>
                      <EuiDescriptionListTitle>{key}</EuiDescriptionListTitle>
                      <EuiDescriptionListDescription>
                        {typeof value === "object"
                          ? JSON.stringify(value, null, 2)
                          : String(value)}
                      </EuiDescriptionListDescription>
                    </React.Fragment>
                  ))}
              </EuiDescriptionList>
            </>
          )}
      </EuiPanel>

      <EuiSpacer size="l" />

      <EuiTitle size="xs">
        <h3>Feature Views Using This Engine</h3>
      </EuiTitle>
      <EuiSpacer size="s" />
      {featureViewInfos.length > 0 ? (
        <EuiBasicTable
          columns={fvColumns}
          items={featureViewInfos}
          rowProps={(item: any) => ({
            "data-test-subj": `row-${item.name}`,
          })}
        />
      ) : (
        <EuiText size="s" color="subdued">
          No feature views found in this project.
        </EuiText>
      )}
    </React.Fragment>
  );
};

const JobsTab = ({
  engineInfo,
  featureViewInfos,
  projectName,
}: {
  engineInfo: any;
  featureViewInfos: FeatureViewEngineInfo[];
  projectName: string | undefined;
}) => {
  const [statusFilter, setStatusFilter] = useState<string | null>(null);

  const jobs = useMemo(
    () => buildJobsFromFeatureViews(featureViewInfos),
    [featureViewInfos],
  );

  const filteredJobs = statusFilter
    ? jobs.filter((j) => j.status === statusFilter)
    : jobs;

  const succeededCount = jobs.filter((j) => j.status === "SUCCEEDED").length;
  const failedCount = jobs.filter((j) => j.status === "ERROR").length;
  const runningCount = jobs.filter((j) => j.status === "RUNNING").length;

  const columns = [
    {
      name: "Job ID",
      field: "id",
      sortable: true,
      render: (id: string) => (
        <EuiText size="xs">
          <code>{id}</code>
        </EuiText>
      ),
    },
    {
      name: "Feature View",
      field: "featureView",
      sortable: true,
      render: (name: string) => (
        <EuiCustomLink to={`/p/${projectName}/feature-view/${name}`}>
          {name}
        </EuiCustomLink>
      ),
    },
    {
      name: "Engine",
      field: "id",
      render: () => (
        <EuiBadge color="hollow">{engineInfo?.engineType || "local"}</EuiBadge>
      ),
    },
    {
      name: "Status",
      field: "status",
      sortable: true,
      render: (status: string) => (
        <EuiHealth color={statusColorMap[status] || "subdued"}>
          {status}
        </EuiHealth>
      ),
    },
    {
      name: "Range Start",
      field: "rangeStart",
      sortable: true,
    },
    {
      name: "Range End",
      field: "rangeEnd",
    },
  ];

  return (
    <React.Fragment>
      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiStat
            title={String(jobs.length)}
            description="Total Jobs"
            titleSize="s"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={String(succeededCount)}
            description="Succeeded"
            titleSize="s"
            titleColor="success"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={String(runningCount)}
            description="Running"
            titleSize="s"
            titleColor="primary"
          />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiStat
            title={String(failedCount)}
            description="Failed"
            titleSize="s"
            titleColor="danger"
          />
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="l" />

      <EuiFlexGroup alignItems="center">
        <EuiFlexItem grow={false}>
          <EuiTitle size="xxs">
            <h4>Filter by Status</h4>
          </EuiTitle>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiFilterGroup>
            <EuiFilterButton
              hasActiveFilters={statusFilter === null}
              onClick={() => setStatusFilter(null)}
            >
              All
            </EuiFilterButton>
            <EuiFilterButton
              hasActiveFilters={statusFilter === "SUCCEEDED"}
              onClick={() =>
                setStatusFilter(
                  statusFilter === "SUCCEEDED" ? null : "SUCCEEDED",
                )
              }
              numFilters={succeededCount}
            >
              Succeeded
            </EuiFilterButton>
            <EuiFilterButton
              hasActiveFilters={statusFilter === "RUNNING"}
              onClick={() =>
                setStatusFilter(statusFilter === "RUNNING" ? null : "RUNNING")
              }
              numFilters={runningCount}
            >
              Running
            </EuiFilterButton>
            <EuiFilterButton
              hasActiveFilters={statusFilter === "ERROR"}
              onClick={() =>
                setStatusFilter(statusFilter === "ERROR" ? null : "ERROR")
              }
              numFilters={failedCount}
            >
              Failed
            </EuiFilterButton>
          </EuiFilterGroup>
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />

      {filteredJobs.length > 0 ? (
        <EuiBasicTable
          columns={columns}
          items={filteredJobs}
          rowProps={(item: MaterializationJobRow) => ({
            "data-test-subj": `row-${item.id}`,
          })}
        />
      ) : (
        <EuiText size="s" color="subdued">
          {jobs.length === 0
            ? "No materialization jobs found. Run 'feast materialize' to create jobs."
            : "No jobs match the selected filter."}
        </EuiText>
      )}
    </React.Fragment>
  );
};

const Index = () => {
  const { projectName } = useParams();
  const navigate = useNavigate();

  const { isLoading, isSuccess, isError, engineInfo, featureViewInfos } =
    useLoadComputeEngine(projectName);

  useDocumentTitle(`Compute & Jobs | Feast`);

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={ComputeEngineIcon}
        pageTitle="Compute & Jobs"
        description={
          projectName !== "all"
            ? "Compute engine configuration and jobs history"
            : "Compute engines and jobs across all projects"
        }
        tabs={[
          {
            label: "Overview",
            isSelected: useMatchExact(""),
            onClick: () => navigate(""),
          },
          {
            label: "Jobs",
            isSelected: useMatchSubpath("jobs"),
            onClick: () => navigate("jobs"),
          },
        ]}
      />
      <EuiPageTemplate.Section>
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && (
          <Routes>
            <Route
              path="/"
              element={
                <OverviewTab
                  engineInfo={engineInfo}
                  featureViewInfos={featureViewInfos}
                  projectName={projectName}
                />
              }
            />
            <Route
              path="jobs"
              element={
                <JobsTab
                  engineInfo={engineInfo}
                  featureViewInfos={featureViewInfos}
                  projectName={projectName}
                />
              }
            />
          </Routes>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default Index;
