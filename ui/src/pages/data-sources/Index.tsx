import React, { useMemo, useState } from "react";
import { useParams } from "react-router-dom";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiFlexGroup,
  EuiFlexItem,
  EuiTitle,
  EuiFieldSearch,
  EuiSpacer,
  EuiButton,
  EuiCallOut,
  EuiButtonGroup,
  EuiSelect,
  EuiText,
  EuiBadge,
  EuiButtonEmpty,
  EuiEmptyPrompt,
} from "@elastic/eui";

import DataSourcesListingTable from "./DataSourcesListingTable";
import DataSourcesCardGrid from "./DataSourcesCardGrid";
import DataSourceCatalog from "./DataSourceCatalog";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import { DataSourceIcon } from "../../graphics/DataSourceIcon";
import { feast } from "../../protos";
import ExportButton from "../../components/ExportButton";
import DataSourceFormModal, {
  DataSourceFormData,
} from "../../components/DataSourceFormModal";
import { useApplyDataSource } from "../../queries/mutations/useDataSourceMutations";
import useResourceQuery, {
  dataSourceListPath,
} from "../../queries/useResourceQuery";
import {
  getDataSourceTypeMeta,
  getConnectionTarget,
} from "./dataSourceTypeUtils";

const useLoadDatasources = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "data-sources-list",
    project: projectName,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources,
  });
};

const SORT_OPTIONS = [
  { value: "name_asc", text: "Name (A → Z)" },
  { value: "name_desc", text: "Name (Z → A)" },
  { value: "type_asc", text: "Type (A → Z)" },
  { value: "created_desc", text: "Newest first" },
  { value: "created_asc", text: "Oldest first" },
];

const VIEW_TOGGLE_BUTTONS = [
  { id: "cards", label: "Cards", iconType: "grid" },
  { id: "table", label: "Table", iconType: "list" },
];

function getCreatedTs(ds: any): number {
  const ts =
    ds.meta?.createdTimestamp ||
    ds.meta?.created_timestamp ||
    ds.spec?.meta?.createdTimestamp;
  if (!ts) return 0;
  const n = new Date(ts).getTime();
  return Number.isNaN(n) ? 0 : n;
}

const formDataToPayload = (formData: DataSourceFormData, project: string) => {
  const payload: Record<string, any> = {
    name: formData.name,
    project,
    type: parseInt(formData.sourceType, 10),
    timestamp_field: formData.timestampField,
    created_timestamp_column: formData.createdTimestampColumn,
    description: formData.description,
    owner: formData.owner,
    tags: Object.fromEntries(
      formData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
    ),
  };

  const st = formData.sourceType;
  if (st === String(feast.core.DataSource.SourceType.BATCH_FILE)) {
    payload.file_options = {
      uri: formData.fileUri,
      file_format: formData.fileFormat || "parquet",
      s3_endpoint_override: formData.fileS3EndpointOverride || "",
    };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_BIGQUERY)) {
    payload.bigquery_options = {
      table: formData.bigqueryTable,
      query: formData.bigqueryQuery,
    };
    if (formData.bigqueryDatePartitionColumn) {
      payload.date_partition_column = formData.bigqueryDatePartitionColumn;
    }
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE)) {
    payload.snowflake_options = {
      table: formData.snowflakeTable,
      database: formData.snowflakeDatabase,
      schema_: formData.snowflakeSchema,
      query: formData.snowflakeQuery || "",
      warehouse: formData.snowflakeWarehouse || "",
    };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_REDSHIFT)) {
    payload.redshift_options = {
      table: formData.redshiftTable,
      database: formData.redshiftDatabase,
      schema_: formData.redshiftSchema,
      query: formData.redshiftQuery || "",
    };
  } else if (st === String(feast.core.DataSource.SourceType.STREAM_KAFKA)) {
    payload.kafka_options = {
      kafka_bootstrap_servers: formData.kafkaBootstrapServers,
      topic: formData.kafkaTopic,
      message_format: formData.kafkaMessageFormat || "json",
      watermark_delay_threshold: formData.kafkaWatermarkDelay || "",
    };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_SPARK)) {
    payload.spark_options = {
      table: formData.sparkTable,
      path: formData.sparkPath,
      query: formData.sparkQuery || "",
      file_format: formData.sparkFileFormat || "",
      table_format: formData.sparkTableFormat || "",
      table_format_catalog: formData.sparkTableFormatCatalog || "",
      table_format_namespace: formData.sparkTableFormatNamespace || "",
      table_format_properties: formData.sparkTableFormatProperties || "",
      date_partition_column: formData.sparkDatePartitionColumn || "",
      date_partition_column_format: formData.sparkDatePartitionFormat || "",
    };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_TRINO)) {
    payload.trino_options = {
      table: formData.trinoTable,
      query: formData.trinoQuery,
    };
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_ATHENA)) {
    payload.athena_options = {
      table: formData.athenaTable,
      query: formData.athenaQuery,
      database: formData.athenaDatabase,
      data_source: formData.athenaDataSource,
    };
    if (formData.athenaDatePartitionColumn) {
      payload.date_partition_column = formData.athenaDatePartitionColumn;
    }
  } else if (st === String(feast.core.DataSource.SourceType.BATCH_ICEBERG)) {
    const catalogProps = formData.icebergCatalogProperties.trim()
      ? JSON.parse(formData.icebergCatalogProperties)
      : {};
    payload.custom_options = {
      configuration: JSON.stringify({
        catalog_type: formData.icebergCatalogType || "rest",
        endpoint: formData.icebergEndpoint,
        warehouse: formData.icebergWarehouse,
        namespace: formData.icebergNamespace,
        table: formData.icebergTable,
        token_env_var: formData.icebergTokenEnvVar || null,
        credential_vending: formData.icebergCredentialVending !== "false",
        catalog_properties: catalogProps,
      }),
    };
    payload.data_source_class_type =
      "feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source.IcebergSource";
  } else if (st === String(feast.core.DataSource.SourceType.STREAM_KINESIS)) {
    payload.kinesis_options = {
      region: formData.kinesisRegion,
      stream_name: formData.kinesisStreamName,
      record_format: formData.kinesisRecordFormat || "json",
    };
  } else if (st === String(feast.core.DataSource.SourceType.CUSTOM_SOURCE)) {
    payload.custom_options = {
      class_name: formData.customSourceClassName,
      config: formData.customSourceConfig,
    };
  }

  return payload;
};

const Index = () => {
  const { projectName } = useParams();
  const { isLoading, isSuccess, isError, isPermissionDenied, data } =
    useLoadDatasources();
  const isAllProjects = projectName === "all";

  const [showCatalog, setShowCatalog] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [preselectedSourceType, setPreselectedSourceType] = useState<
    string | null
  >(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [searchString, setSearchString] = useState("");
  const [sortBy, setSortBy] = useState("name_asc");
  const [viewMode, setViewMode] = useState("cards");
  const [typeFilter, setTypeFilter] = useState<string>("all");
  const applyDataSource = useApplyDataSource();

  useDocumentTitle(`Data Sources | Feast`);

  // Summary stats + available filter values
  const stats = useMemo(() => {
    const empty = {
      total: 0,
      types: [] as Array<{ id: string; label: string }>,
    };
    if (!data) return empty;
    const typeMap = new Map<string, string>();
    data.forEach((ds: any) => {
      const meta = getDataSourceTypeMeta(ds);
      typeMap.set(meta.id, meta.label);
    });
    const types = Array.from(typeMap.entries())
      .map(([id, label]) => ({ id, label }))
      .sort((a, b) => a.label.localeCompare(b.label));
    return { total: data.length, types };
  }, [data]);

  const processedData = useMemo(() => {
    if (!data) return [] as any[];
    let filtered = data;

    if (typeFilter !== "all") {
      filtered = filtered.filter(
        (ds: any) => getDataSourceTypeMeta(ds).id === typeFilter,
      );
    }

    if (searchString.trim()) {
      const q = searchString.toLowerCase();
      filtered = filtered.filter((ds: any) => {
        const spec = ds.spec || ds;
        const meta = getDataSourceTypeMeta(ds);
        const connection = getConnectionTarget(ds);
        const haystack = [
          spec.name || ds.name || "",
          spec.description || "",
          spec.owner || "",
          meta.label,
          connection?.value || "",
          JSON.stringify(spec.tags || {}),
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(q);
      });
    }

    const [key, order] = sortBy.split("_");
    filtered = [...filtered].sort((a, b) => {
      let aVal: any;
      let bVal: any;
      if (key === "name") {
        aVal = (a.spec?.name || a.name || "").toLowerCase();
        bVal = (b.spec?.name || b.name || "").toLowerCase();
      } else if (key === "type") {
        aVal = getDataSourceTypeMeta(a).label.toLowerCase();
        bVal = getDataSourceTypeMeta(b).label.toLowerCase();
      } else {
        aVal = getCreatedTs(a);
        bVal = getCreatedTs(b);
      }
      if (typeof aVal === "number" && typeof bVal === "number") {
        return order === "asc" ? aVal - bVal : bVal - aVal;
      }
      const cmp = String(aVal).localeCompare(String(bVal));
      return order === "asc" ? cmp : -cmp;
    });

    return filtered;
  }, [data, searchString, sortBy, typeFilter]);

  const hasExistingSources = isSuccess && data && data.length > 0;
  const isEmpty = isSuccess && (!data || data.length === 0);
  const isFiltering = searchString.trim() !== "" || typeFilter !== "all";

  const clearFilters = () => {
    setSearchString("");
    setTypeFilter("all");
  };

  const modalInitialData = useMemo(() => {
    if (!preselectedSourceType) return undefined;
    return {
      name: "",
      description: "",
      owner: "",
      sourceType: preselectedSourceType,
      timestampField: "",
      createdTimestampColumn: "",
      tags: [] as { key: string; value: string }[],
      fileUri: "",
      fileFormat: "parquet",
      fileS3EndpointOverride: "",
      bigqueryTable: "",
      bigqueryQuery: "",
      bigqueryDatePartitionColumn: "",
      snowflakeTable: "",
      snowflakeDatabase: "",
      snowflakeSchema: "",
      snowflakeQuery: "",
      snowflakeWarehouse: "",
      redshiftTable: "",
      redshiftDatabase: "",
      redshiftSchema: "",
      redshiftQuery: "",
      kafkaBootstrapServers: "",
      kafkaTopic: "",
      kafkaMessageFormat: "json",
      kafkaWatermarkDelay: "",
      sparkTable: "",
      sparkPath: "",
      sparkQuery: "",
      sparkFileFormat: "parquet",
      sparkTableFormat: "",
      sparkTableFormatCatalog: "",
      sparkTableFormatNamespace: "",
      sparkTableFormatProperties: "",
      sparkDatePartitionColumn: "",
      sparkDatePartitionFormat: "%Y-%m-%d",
      kinesisRegion: "",
      kinesisStreamName: "",
      kinesisRecordFormat: "json",
      trinoTable: "",
      trinoQuery: "",
      athenaTable: "",
      athenaQuery: "",
      athenaDatabase: "",
      athenaDataSource: "",
      athenaDatePartitionColumn: "",
      customSourceClassName: "",
      customSourceConfig: "",
      icebergCatalogType: "rest",
      icebergEndpoint: "",
      icebergWarehouse: "",
      icebergNamespace: "",
      icebergTable: "",
      icebergTokenEnvVar: "",
      icebergCredentialVending: "true",
      icebergCatalogProperties: "",
      rayReaderType: "parquet",
      rayPath: "",
      rayReaderOptions: "",
      postgresTable: "",
      postgresQuery: "",
      mongodbCollection: "",
      clickhouseTable: "",
      clickhouseQuery: "",
      mssqlTable: "",
      mssqlConnectionStr: "",
      mssqlDatePartitionColumn: "",
      oracleTable: "",
      oracleConnectionStr: "",
      oracleDatePartitionColumn: "",
      couchbaseDatabase: "",
      couchbaseScope: "",
      couchbaseCollection: "",
      couchbaseQuery: "",
    };
  }, [preselectedSourceType]);

  const handleSelectType = (sourceType: string) => {
    setPreselectedSourceType(sourceType);
    setIsModalOpen(true);
  };

  const handleCreateSubmit = (formData: DataSourceFormData) => {
    const payload = formDataToPayload(formData, projectName || "");
    applyDataSource.mutate(payload as any, {
      onSuccess: () => {
        setIsModalOpen(false);
        setPreselectedSourceType(null);
        setShowCatalog(false);
        setErrorMessage(null);
        setSuccessMessage(
          `Data source "${formData.name}" created successfully.`,
        );
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setErrorMessage(message);
      },
    });
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DataSourceIcon}
        pageTitle="Data Sources"
        rightSideItems={[
          ...(isAllProjects || showCatalog
            ? []
            : [
                <EuiButton
                  fill
                  iconType="plus"
                  onClick={() => setShowCatalog(true)}
                  key="create"
                >
                  Create Data Source
                </EuiButton>,
              ]),
          <ExportButton
            data={processedData ?? []}
            fileName="data_sources"
            formats={["json"]}
            key="export"
          />,
        ]}
      />
      <EuiPageTemplate.Section>
        {successMessage && (
          <>
            <EuiCallOut
              title={successMessage}
              color="success"
              iconType="check"
              size="s"
            />
            <EuiSpacer size="m" />
          </>
        )}
        {errorMessage && !isModalOpen && (
          <>
            <EuiCallOut
              title={errorMessage}
              color="danger"
              iconType="alert"
              size="s"
            />
            <EuiSpacer size="m" />
          </>
        )}

        {showCatalog && !isAllProjects && (
          <>
            <EuiFlexGroup alignItems="center" justifyContent="spaceBetween">
              <EuiFlexItem grow={false}>
                <EuiTitle size="s">
                  <h2>Select a Data Source Type</h2>
                </EuiTitle>
              </EuiFlexItem>
              {hasExistingSources && (
                <EuiFlexItem grow={false}>
                  <EuiButton
                    onClick={() => setShowCatalog(false)}
                    iconType="arrowLeft"
                    size="s"
                  >
                    Back to Data Sources
                  </EuiButton>
                </EuiFlexItem>
              )}
            </EuiFlexGroup>
            <EuiSpacer size="m" />
            <DataSourceCatalog onSelectType={handleSelectType} />
          </>
        )}

        {!showCatalog && (
          <>
            {isLoading && (
              <p>
                <EuiLoadingSpinner size="m" /> Loading
              </p>
            )}
            {isPermissionDenied && (
              <EuiCallOut
                title="Permission denied"
                color="warning"
                iconType="lock"
              >
                <p>You do not have permission to view data sources.</p>
              </EuiCallOut>
            )}
            {isError && !isPermissionDenied && (
              <p>We encountered an error while loading.</p>
            )}
            {isEmpty && !isAllProjects && (
              <>
                <EuiTitle size="s">
                  <h2>No data sources yet — create your first connection</h2>
                </EuiTitle>
                <EuiSpacer size="l" />
                <DataSourceCatalog onSelectType={handleSelectType} />
              </>
            )}
            {isEmpty && isAllProjects && (
              <p>No data sources found across projects.</p>
            )}
            {hasExistingSources && (
              <React.Fragment>
                {/* Stats + view toggle */}
                <EuiFlexGroup
                  justifyContent="spaceBetween"
                  alignItems="center"
                  gutterSize="m"
                >
                  <EuiFlexItem grow={false}>
                    <EuiText size="xs" color="subdued">
                      <strong>{stats.total}</strong> data source
                      {stats.total === 1 ? "" : "s"}
                      {stats.types.length > 1 && (
                        <>
                          {" "}
                          across <strong>{stats.types.length}</strong> types
                        </>
                      )}
                    </EuiText>
                  </EuiFlexItem>
                  <EuiFlexItem grow={false}>
                    <EuiButtonGroup
                      legend="View mode"
                      options={VIEW_TOGGLE_BUTTONS}
                      idSelected={viewMode}
                      onChange={(id) => setViewMode(id)}
                      isIconOnly
                      buttonSize="compressed"
                    />
                  </EuiFlexItem>
                </EuiFlexGroup>

                <EuiSpacer size="m" />

                {/* Search + filters + sort toolbar */}
                <EuiFlexGroup alignItems="center" gutterSize="m">
                  <EuiFlexItem>
                    <EuiFieldSearch
                      placeholder="Search by name, description, type, owner, connection, tags..."
                      value={searchString}
                      onChange={(e) => setSearchString(e.target.value)}
                      isClearable
                      fullWidth
                    />
                  </EuiFlexItem>
                  {stats.types.length > 1 && (
                    <EuiFlexItem grow={false} style={{ width: 200 }}>
                      <EuiSelect
                        options={[
                          { value: "all", text: "All types" },
                          ...stats.types.map((t) => ({
                            value: t.id,
                            text: t.label,
                          })),
                        ]}
                        value={typeFilter}
                        onChange={(e) => setTypeFilter(e.target.value)}
                        compressed
                        prepend="Type"
                      />
                    </EuiFlexItem>
                  )}
                  <EuiFlexItem grow={false} style={{ width: 180 }}>
                    <EuiSelect
                      options={SORT_OPTIONS}
                      value={sortBy}
                      onChange={(e) => setSortBy(e.target.value)}
                      compressed
                      prepend="Sort"
                    />
                  </EuiFlexItem>
                </EuiFlexGroup>

                <EuiSpacer size="m" />

                {isFiltering && (
                  <>
                    <EuiFlexGroup
                      gutterSize="s"
                      alignItems="center"
                      wrap
                      responsive={false}
                    >
                      <EuiFlexItem grow={false}>
                        <EuiText size="xs" color="subdued">
                          Showing {processedData.length} of {data?.length}
                        </EuiText>
                      </EuiFlexItem>
                      {searchString.trim() && (
                        <EuiFlexItem grow={false}>
                          <EuiBadge
                            color="hollow"
                            iconType="cross"
                            iconSide="right"
                            iconOnClick={() => setSearchString("")}
                            iconOnClickAriaLabel="Clear search filter"
                          >
                            Search: “{searchString.trim()}”
                          </EuiBadge>
                        </EuiFlexItem>
                      )}
                      {typeFilter !== "all" && (
                        <EuiFlexItem grow={false}>
                          <EuiBadge
                            color="hollow"
                            iconType="cross"
                            iconSide="right"
                            iconOnClick={() => setTypeFilter("all")}
                            iconOnClickAriaLabel="Clear type filter"
                          >
                            Type:{" "}
                            {stats.types.find((t) => t.id === typeFilter)
                              ?.label || typeFilter}
                          </EuiBadge>
                        </EuiFlexItem>
                      )}
                      <EuiFlexItem grow={false}>
                        <EuiButtonEmpty
                          size="xs"
                          onClick={clearFilters}
                          flush="left"
                        >
                          Clear all
                        </EuiButtonEmpty>
                      </EuiFlexItem>
                    </EuiFlexGroup>
                    <EuiSpacer size="s" />
                  </>
                )}

                {isFiltering && processedData.length === 0 ? (
                  <EuiEmptyPrompt
                    iconType="search"
                    title={<h3>No matching data sources</h3>}
                    body={
                      <p>
                        No data sources match your current search and filters.
                      </p>
                    }
                    actions={
                      <EuiButton onClick={clearFilters}>
                        Clear filters
                      </EuiButton>
                    }
                  />
                ) : (
                  <>
                    {viewMode === "cards" && (
                      <DataSourcesCardGrid dataSources={processedData} />
                    )}
                    {viewMode === "table" && (
                      <DataSourcesListingTable dataSources={processedData} />
                    )}
                  </>
                )}
              </React.Fragment>
            )}
          </>
        )}
      </EuiPageTemplate.Section>

      {isModalOpen && (
        <DataSourceFormModal
          onClose={() => {
            setIsModalOpen(false);
            setPreselectedSourceType(null);
            setErrorMessage(null);
          }}
          onSubmit={handleCreateSubmit}
          isSubmitting={applyDataSource.isLoading}
          submitError={errorMessage}
          initialData={modalInitialData}
        />
      )}
    </EuiPageTemplate>
  );
};

export default Index;
