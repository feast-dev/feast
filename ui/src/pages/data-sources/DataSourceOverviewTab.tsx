import React from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiHorizontalRule,
  EuiLoadingSpinner,
  EuiText,
  EuiTitle,
  EuiPanel,
  EuiDescriptionList,
  EuiDescriptionListTitle,
  EuiDescriptionListDescription,
  EuiSpacer,
  EuiBadge,
  EuiCallOut,
  EuiCodeBlock,
  EuiToolTip,
  EuiIcon,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import FeatureViewEdgesList from "../entities/FeatureViewEdgesList";
import RequestDataSourceSchemaTable from "./RequestDataSourceSchemaTable";
import useLoadDataSource from "./useLoadDataSource";
import ConnectionStatusPanel from "./ConnectionStatusPanel";
import { toDate } from "../../utils/timestamp";
import {
  getDataSourceTypeMeta,
  getRelativeTime,
  CATEGORY_META,
  getTagBadge,
} from "./dataSourceTypeUtils";

/* ────── connection configuration rows ────── */

interface ConfigRow {
  title: string;
  value: string;
  code?: boolean;
  codeLang?: string;
}

function parseIceberg(ds: any): ConfigRow[] {
  try {
    const cfg = JSON.parse(ds.customOptions?.configuration || "{}");
    const rows: ConfigRow[] = [
      { title: "Catalog Type", value: cfg.catalog_type || "rest" },
    ];
    if (cfg.endpoint) rows.push({ title: "Endpoint", value: cfg.endpoint });
    if (cfg.warehouse) rows.push({ title: "Warehouse", value: cfg.warehouse });
    if (cfg.namespace) rows.push({ title: "Namespace", value: cfg.namespace });
    if (cfg.table) rows.push({ title: "Table", value: cfg.table });
    if (cfg.token_env_var)
      rows.push({ title: "Token Env Variable", value: cfg.token_env_var });
    return rows;
  } catch {
    return [];
  }
}

function getConnectionRows(ds: any): ConfigRow[] {
  const rows: ConfigRow[] = [];
  const push = (
    title: string,
    value?: string | null,
    opts?: Partial<ConfigRow>,
  ) => {
    if (value) rows.push({ title, value, ...opts });
  };

  if (ds.fileOptions) {
    push("File URI", ds.fileOptions.uri);
    push(
      "File Format",
      ds.fileOptions.fileFormat?.fileFormat || ds.fileOptions.fileFormat,
    );
    push("S3 Endpoint Override", ds.fileOptions.s3EndpointOverride);
  } else if (ds.bigqueryOptions) {
    push("Table", ds.bigqueryOptions.table);
    push("Query", ds.bigqueryOptions.query, { code: true, codeLang: "sql" });
  } else if (ds.snowflakeOptions) {
    push("Database", ds.snowflakeOptions.database);
    push("Schema", ds.snowflakeOptions.schema);
    push("Table", ds.snowflakeOptions.table);
    push("Warehouse", ds.snowflakeOptions.warehouse);
    push("Query", ds.snowflakeOptions.query, { code: true, codeLang: "sql" });
  } else if (ds.redshiftOptions) {
    push("Database", ds.redshiftOptions.database);
    push("Schema", ds.redshiftOptions.schema);
    push("Table", ds.redshiftOptions.table);
    push("Query", ds.redshiftOptions.query, { code: true, codeLang: "sql" });
  } else if (ds.sparkOptions) {
    push("Table", ds.sparkOptions.table);
    push("Path", ds.sparkOptions.path);
    push("File Format", ds.sparkOptions.fileFormat);
    push("Query", ds.sparkOptions.query, { code: true, codeLang: "sql" });
  } else if (ds.trinoOptions) {
    push("Table", ds.trinoOptions.table);
    push("Query", ds.trinoOptions.query, { code: true, codeLang: "sql" });
  } else if (ds.athenaOptions) {
    push("Database", ds.athenaOptions.database);
    push("Data Source", ds.athenaOptions.dataSource);
    push("Table", ds.athenaOptions.table);
    push("Query", ds.athenaOptions.query, { code: true, codeLang: "sql" });
  } else if (ds.kafkaOptions) {
    push("Bootstrap Servers", ds.kafkaOptions.kafkaBootstrapServers);
    push("Topic", ds.kafkaOptions.topic);
    push(
      "Message Format",
      ds.kafkaOptions.messageFormat?.jsonFormat
        ? "json"
        : ds.kafkaOptions.messageFormat,
    );
    push("Watermark Delay", ds.kafkaOptions.watermarkDelayThreshold);
  } else if (ds.kinesisOptions) {
    push("Region", ds.kinesisOptions.region);
    push("Stream Name", ds.kinesisOptions.streamName);
  } else if (ds.customOptions?.configuration) {
    return parseIceberg(ds);
  } else if (ds.customOptions) {
    push("Class Name", ds.customOptions.className);
    push("Config", ds.customOptions.config, { code: true, codeLang: "json" });
  }

  return rows;
}

/* ────── main ────── */

const DataSourceOverviewTab = () => {
  const { dataSourceName, projectName } = useParams();

  const dsName = dataSourceName === undefined ? "" : dataSourceName;
  const { isLoading, isSuccess, isError, data, consumingFeatureViews } =
    useLoadDataSource(dsName);
  const isEmpty = data === undefined;

  const viewTypesForDs: Record<string, string> | undefined =
    consumingFeatureViews && consumingFeatureViews.length > 0
      ? consumingFeatureViews.reduce((acc: Record<string, string>, f: any) => {
          acc[f.target.name] =
            f.target.type === "labelView" ? "labelView" : "featureView";
          return acc;
        }, {})
      : undefined;

  if (isLoading) {
    return (
      <>
        <EuiLoadingSpinner size="m" /> Loading
      </>
    );
  }
  if (isEmpty) {
    return <p>No data source with name: {dataSourceName}</p>;
  }
  if (isError) {
    return (
      <EuiCallOut
        title="Error loading data source"
        color="danger"
        iconType="alert"
      >
        <p>Could not load data source {dataSourceName}.</p>
      </EuiCallOut>
    );
  }
  if (!isSuccess || !data) return null;

  const spec = data.spec || data;
  const meta = getDataSourceTypeMeta(data);
  const categoryInfo = CATEGORY_META[meta.category];
  const Icon = meta.icon;
  const description = spec.description || "";
  const owner = spec.owner || "";
  const timestampField = spec.timestampField || spec.timestamp_field || "";
  const createdTsCol =
    spec.createdTimestampColumn || spec.created_timestamp_column || "";
  const tags: Record<string, string> = spec.tags || {};
  const connectionRows = getConnectionRows(spec);

  const latestEvent = spec.meta?.latestEventTimestamp;
  const earliestEvent = spec.meta?.earliestEventTimestamp;
  const createdTs = data.meta?.createdTimestamp || data.meta?.created_timestamp;
  const lastUpdatedTs =
    data.meta?.lastUpdatedTimestamp || data.meta?.last_updated_timestamp;

  const fmtDate = (ts: any) => {
    try {
      return toDate(ts).toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "numeric",
      });
    } catch {
      return "—";
    }
  };

  return (
    <>
      {/* Hero header */}
      <EuiPanel hasBorder style={{ borderLeft: `4px solid ${meta.color}` }}>
        <EuiFlexGroup alignItems="center" gutterSize="m" responsive={false}>
          <EuiFlexItem grow={false}>
            <div
              style={{
                width: 56,
                height: 56,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                borderRadius: 10,
                backgroundColor: `${meta.color}14`,
              }}
            >
              <Icon width={36} height={36} />
            </div>
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiFlexGroup
              gutterSize="s"
              alignItems="center"
              responsive={false}
              wrap
            >
              <EuiFlexItem grow={false}>
                <EuiTitle size="xs">
                  <h2 style={{ margin: 0 }}>{meta.label}</h2>
                </EuiTitle>
              </EuiFlexItem>
              <EuiFlexItem grow={false}>
                <EuiBadge color={categoryInfo.color as any}>
                  {categoryInfo.label}
                </EuiBadge>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiText size="s" color="subdued">
              <p style={{ margin: "4px 0 0" }}>
                {description || meta.description}
              </p>
            </EuiText>
          </EuiFlexItem>
        </EuiFlexGroup>
      </EuiPanel>

      <EuiSpacer size="m" />

      <EuiFlexGroup gutterSize="l">
        {/* Left: connection config + test + schema */}
        <EuiFlexItem grow={1}>
          <EuiPanel hasBorder>
            <EuiTitle size="xs">
              <h2>Connection</h2>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {connectionRows.length > 0 ? (
              <EuiDescriptionList
                type="column"
                compressed
                columnWidths={[1, 2]}
              >
                {connectionRows.map((row) => (
                  <React.Fragment key={row.title}>
                    <EuiDescriptionListTitle>
                      {row.title}
                    </EuiDescriptionListTitle>
                    <EuiDescriptionListDescription>
                      {row.code ? (
                        <EuiCodeBlock
                          language={row.codeLang || "text"}
                          paddingSize="s"
                          fontSize="s"
                          isCopyable
                        >
                          {row.value}
                        </EuiCodeBlock>
                      ) : (
                        <code style={{ fontSize: 12, wordBreak: "break-all" }}>
                          {row.value}
                        </code>
                      )}
                    </EuiDescriptionListDescription>
                  </React.Fragment>
                ))}
              </EuiDescriptionList>
            ) : (
              <EuiText size="s" color="subdued">
                No connection configuration available for this source.
              </EuiText>
            )}
          </EuiPanel>

          <EuiSpacer size="m" />
          <ConnectionStatusPanel
            dataSourceName={dsName}
            projectName={projectName || ""}
          />

          {spec.requestDataOptions && (
            <>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder>
                <EuiTitle size="xs">
                  <h2>Request Source Schema</h2>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <RequestDataSourceSchemaTable
                  fields={(spec.requestDataOptions?.schema || []).map(
                    (obj: any) => ({
                      fieldName: obj.name!,
                      valueType: obj.valueType!,
                    }),
                  )}
                />
              </EuiPanel>
            </>
          )}
        </EuiFlexItem>

        {/* Right: metadata + consuming views */}
        <EuiFlexItem grow={1}>
          <EuiPanel hasBorder grow={false}>
            <EuiTitle size="xs">
              <h3>Properties</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            <EuiDescriptionList compressed>
              <EuiDescriptionListTitle>Type</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <span
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 6,
                  }}
                >
                  <Icon width={16} height={16} /> {meta.label}
                </span>
              </EuiDescriptionListDescription>

              <EuiDescriptionListTitle>Category</EuiDescriptionListTitle>
              <EuiDescriptionListDescription>
                <EuiBadge color={categoryInfo.color as any}>
                  {categoryInfo.label}
                </EuiBadge>
              </EuiDescriptionListDescription>

              {owner && (
                <>
                  <EuiDescriptionListTitle>Owner</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {owner}
                  </EuiDescriptionListDescription>
                </>
              )}

              {timestampField && (
                <>
                  <EuiDescriptionListTitle>
                    Event Timestamp Column
                  </EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    <code>{timestampField}</code>
                  </EuiDescriptionListDescription>
                </>
              )}

              {createdTsCol && (
                <>
                  <EuiDescriptionListTitle>
                    Created Timestamp Column
                  </EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    <code>{createdTsCol}</code>
                  </EuiDescriptionListDescription>
                </>
              )}

              {(earliestEvent || latestEvent) && (
                <>
                  <EuiDescriptionListTitle>
                    Event Time Range
                  </EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    {earliestEvent ? fmtDate(earliestEvent) : "—"} →{" "}
                    {latestEvent ? fmtDate(latestEvent) : "—"}
                  </EuiDescriptionListDescription>
                </>
              )}

              {createdTs && (
                <>
                  <EuiDescriptionListTitle>Created</EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    <EuiToolTip content={fmtDate(createdTs)}>
                      <span>
                        <EuiIcon type="clock" size="s" />{" "}
                        {getRelativeTime(toDate(createdTs))}
                      </span>
                    </EuiToolTip>
                  </EuiDescriptionListDescription>
                </>
              )}

              {lastUpdatedTs && (
                <>
                  <EuiDescriptionListTitle>
                    Last Updated
                  </EuiDescriptionListTitle>
                  <EuiDescriptionListDescription>
                    <EuiToolTip content={fmtDate(lastUpdatedTs)}>
                      <span>
                        <EuiIcon type="clock" size="s" />{" "}
                        {getRelativeTime(toDate(lastUpdatedTs))}
                      </span>
                    </EuiToolTip>
                  </EuiDescriptionListDescription>
                </>
              )}
            </EuiDescriptionList>
          </EuiPanel>

          {Object.keys(tags).length > 0 && (
            <>
              <EuiSpacer size="m" />
              <EuiPanel hasBorder grow={false}>
                <EuiTitle size="xs">
                  <h3>Tags</h3>
                </EuiTitle>
                <EuiHorizontalRule margin="xs" />
                <EuiFlexGroup gutterSize="xs" wrap responsive={false}>
                  {Object.entries(tags).map(([key, value]) => {
                    const badge = getTagBadge(key, value as string);
                    return (
                      <EuiFlexItem grow={false} key={key}>
                        <EuiBadge color={badge.color as any}>
                          {badge.label}
                        </EuiBadge>
                      </EuiFlexItem>
                    );
                  })}
                </EuiFlexGroup>
              </EuiPanel>
            </>
          )}

          <EuiSpacer size="m" />
          <EuiPanel hasBorder grow={false}>
            <EuiTitle size="xs">
              <h3>Consuming Views ({consumingFeatureViews?.length || 0})</h3>
            </EuiTitle>
            <EuiHorizontalRule margin="xs" />
            {consumingFeatureViews && consumingFeatureViews.length > 0 ? (
              <FeatureViewEdgesList
                fvNames={consumingFeatureViews.map((f: any) => f.target.name)}
                viewTypes={viewTypesForDs}
              />
            ) : (
              <EuiText size="s" color="subdued">
                No feature or label views consume this data source yet.
              </EuiText>
            )}
          </EuiPanel>
        </EuiFlexItem>
      </EuiFlexGroup>
    </>
  );
};

export default DataSourceOverviewTab;
