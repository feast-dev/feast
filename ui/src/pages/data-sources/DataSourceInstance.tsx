import React, { useState } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageTemplate,
  EuiButton,
  EuiButtonEmpty,
  EuiConfirmModal,
} from "@elastic/eui";

import { DataSourceIcon } from "../../graphics/DataSourceIcon";
import { useMatchExact } from "../../hooks/useMatchSubpath";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import DataSourceRawData from "./DataSourceRawData";
import DataSourceOverviewTab from "./DataSourceOverviewTab";
import DataSourceFormModal, {
  DataSourceFormData,
} from "../../components/DataSourceFormModal";
import {
  useApplyDataSource,
  useDeleteDataSource,
} from "../../queries/mutations/useDataSourceMutations";
import useLoadDataSource from "./useLoadDataSource";
import { feast } from "../../protos";

import {
  useDataSourceCustomTabs,
  useDataSourceCustomTabRoutes,
} from "../../custom-tabs/TabsRegistryContext";

const buildEditFormData = (ds: any): DataSourceFormData => {
  const spec = ds.spec || ds;
  const tags = spec.tags
    ? Object.entries(spec.tags).map(([key, value]) => ({
        key,
        value: value as string,
      }))
    : [];

  return {
    name: spec.name || ds.name || "",
    description: spec.description || ds.description || "",
    owner: spec.owner || ds.owner || "",
    sourceType: String(spec.type ?? ds.type ?? 0),
    timestampField: spec.timestampField || ds.timestampField || "",
    createdTimestampColumn:
      spec.createdTimestampColumn || ds.createdTimestampColumn || "",
    tags,
    // File
    fileUri: spec.fileOptions?.uri || ds.fileOptions?.uri || "",
    fileFormat:
      spec.fileOptions?.fileFormat || ds.fileOptions?.fileFormat || "parquet",
    fileS3EndpointOverride:
      spec.fileOptions?.s3EndpointOverride ||
      ds.fileOptions?.s3EndpointOverride ||
      "",
    // BigQuery
    bigqueryTable:
      spec.bigqueryOptions?.table || ds.bigqueryOptions?.table || "",
    bigqueryQuery:
      spec.bigqueryOptions?.query || ds.bigqueryOptions?.query || "",
    bigqueryDatePartitionColumn:
      spec.datePartitionColumn || ds.datePartitionColumn || "",
    // Snowflake
    snowflakeTable:
      spec.snowflakeOptions?.table || ds.snowflakeOptions?.table || "",
    snowflakeDatabase:
      spec.snowflakeOptions?.database || ds.snowflakeOptions?.database || "",
    snowflakeSchema:
      spec.snowflakeOptions?.schema || ds.snowflakeOptions?.schema || "",
    snowflakeQuery:
      spec.snowflakeOptions?.query || ds.snowflakeOptions?.query || "",
    snowflakeWarehouse:
      spec.snowflakeOptions?.warehouse || ds.snowflakeOptions?.warehouse || "",
    // Redshift
    redshiftTable:
      spec.redshiftOptions?.table || ds.redshiftOptions?.table || "",
    redshiftDatabase:
      spec.redshiftOptions?.database || ds.redshiftOptions?.database || "",
    redshiftSchema:
      spec.redshiftOptions?.schema || ds.redshiftOptions?.schema || "",
    redshiftQuery:
      spec.redshiftOptions?.query || ds.redshiftOptions?.query || "",
    // Kafka
    kafkaBootstrapServers:
      spec.kafkaOptions?.kafkaBootstrapServers ||
      ds.kafkaOptions?.kafkaBootstrapServers ||
      "",
    kafkaTopic: spec.kafkaOptions?.topic || ds.kafkaOptions?.topic || "",
    kafkaMessageFormat:
      spec.kafkaOptions?.messageFormat ||
      ds.kafkaOptions?.messageFormat ||
      "json",
    kafkaWatermarkDelay:
      spec.kafkaOptions?.watermarkDelayThreshold ||
      ds.kafkaOptions?.watermarkDelayThreshold ||
      "",
    // Spark
    sparkTable: spec.sparkOptions?.table || ds.sparkOptions?.table || "",
    sparkPath: spec.sparkOptions?.path || ds.sparkOptions?.path || "",
    sparkQuery: spec.sparkOptions?.query || ds.sparkOptions?.query || "",
    sparkFileFormat:
      spec.sparkOptions?.fileFormat || ds.sparkOptions?.fileFormat || "",
    sparkTableFormat:
      spec.sparkOptions?.tableFormat?.formatType ||
      ds.sparkOptions?.tableFormat?.formatType ||
      "",
    sparkTableFormatCatalog:
      spec.sparkOptions?.tableFormat?.catalog ||
      ds.sparkOptions?.tableFormat?.catalog ||
      "",
    sparkTableFormatNamespace:
      spec.sparkOptions?.tableFormat?.namespace ||
      ds.sparkOptions?.tableFormat?.namespace ||
      "",
    sparkTableFormatProperties: (() => {
      const props =
        spec.sparkOptions?.tableFormat?.properties ||
        ds.sparkOptions?.tableFormat?.properties;
      return props ? JSON.stringify(props) : "";
    })(),
    sparkDatePartitionColumn:
      spec.sparkOptions?.datePartitionColumn ||
      ds.sparkOptions?.datePartitionColumn ||
      spec.datePartitionColumn ||
      ds.datePartitionColumn ||
      "",
    sparkDatePartitionFormat:
      spec.sparkOptions?.datePartitionColumnFormat ||
      ds.sparkOptions?.datePartitionColumnFormat ||
      "%Y-%m-%d",
    // Kinesis
    kinesisRegion:
      spec.kinesisOptions?.region || ds.kinesisOptions?.region || "",
    kinesisStreamName:
      spec.kinesisOptions?.streamName || ds.kinesisOptions?.streamName || "",
    kinesisRecordFormat:
      spec.kinesisOptions?.recordFormat ||
      ds.kinesisOptions?.recordFormat ||
      "json",
    // Trino
    trinoTable: spec.trinoOptions?.table || ds.trinoOptions?.table || "",
    trinoQuery: spec.trinoOptions?.query || ds.trinoOptions?.query || "",
    // Athena
    athenaTable: spec.athenaOptions?.table || ds.athenaOptions?.table || "",
    athenaQuery: spec.athenaOptions?.query || ds.athenaOptions?.query || "",
    athenaDatabase:
      spec.athenaOptions?.database || ds.athenaOptions?.database || "",
    athenaDataSource:
      spec.athenaOptions?.dataSource || ds.athenaOptions?.dataSource || "",
    athenaDatePartitionColumn:
      spec.datePartitionColumn || ds.datePartitionColumn || "",
    // Custom
    customSourceClassName:
      spec.customOptions?.className || ds.customOptions?.className || "",
    customSourceConfig:
      spec.customOptions?.config || ds.customOptions?.config || "",
    // Iceberg
    ...(() => {
      const configStr =
        spec.customOptions?.configuration ||
        ds.customOptions?.configuration ||
        "";
      if (
        String(spec.type ?? ds.type ?? 0) ===
          String(feast.core.DataSource.SourceType.BATCH_ICEBERG) &&
        configStr
      ) {
        try {
          const cfg = JSON.parse(configStr);
          return {
            icebergCatalogType: cfg.catalog_type || "rest",
            icebergEndpoint: cfg.endpoint || "",
            icebergWarehouse: cfg.warehouse || "",
            icebergNamespace: cfg.namespace || "",
            icebergTable: cfg.table || "",
            icebergTokenEnvVar: cfg.token_env_var || "",
            icebergCredentialVending: String(cfg.credential_vending ?? true),
            icebergCatalogProperties: cfg.catalog_properties
              ? JSON.stringify(cfg.catalog_properties)
              : "",
          };
        } catch {
          /* ignore parse errors */
        }
      }
      return {
        icebergCatalogType: "rest",
        icebergEndpoint: "",
        icebergWarehouse: "",
        icebergNamespace: "",
        icebergTable: "",
        icebergTokenEnvVar: "",
        icebergCredentialVending: "true",
        icebergCatalogProperties: "",
      };
    })(),
    // Ray
    rayReaderType: "",
    rayPath: "",
    rayReaderOptions: "",
    // Postgres
    postgresTable: "",
    postgresQuery: "",
    // MongoDB
    mongodbCollection: "",
    // ClickHouse
    clickhouseTable: "",
    clickhouseQuery: "",
    // MSSQL
    mssqlTable: "",
    mssqlConnectionStr: "",
    mssqlDatePartitionColumn: "",
    // Oracle
    oracleTable: "",
    oracleConnectionStr: "",
    oracleDatePartitionColumn: "",
    // Couchbase
    couchbaseDatabase: "",
    couchbaseScope: "",
    couchbaseCollection: "",
    couchbaseQuery: "",
  };
};

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

const DataSourceInstance = () => {
  const navigate = useNavigate();
  let { dataSourceName, projectName } = useParams();

  useDocumentTitle(`${dataSourceName} | Data Source | Feast`);

  let tabs = [
    {
      label: "Overview",
      isSelected: useMatchExact(""),
      onClick: () => {
        navigate("");
      },
    },
  ];

  const { customNavigationTabs } = useDataSourceCustomTabs(navigate);
  tabs = tabs.concat(customNavigationTabs);

  const CustomTabRoutes = useDataSourceCustomTabRoutes();

  const { data } = useLoadDataSource(dataSourceName || "");
  const applyDataSource = useApplyDataSource();
  const deleteDataSource = useDeleteDataSource();

  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  const handleDelete = () => {
    deleteDataSource.mutate(
      { name: dataSourceName || "", project: projectName || "" },
      {
        onSuccess: () => {
          navigate(`/p/${projectName}/data-source`);
        },
      },
    );
  };

  const handleEditSubmit = (formData: DataSourceFormData) => {
    const payload = formDataToPayload(formData, projectName || "");
    applyDataSource.mutate(payload as any, {
      onSuccess: () => {
        setIsEditModalOpen(false);
        setEditError(null);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setEditError(message);
      },
    });
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={DataSourceIcon}
        pageTitle={`Data Source: ${dataSourceName}`}
        rightSideItems={[
          <EuiButton
            key="edit"
            iconType="pencil"
            onClick={() => {
              setEditError(null);
              setIsEditModalOpen(true);
            }}
          >
            Edit
          </EuiButton>,
          <EuiButtonEmpty
            key="delete"
            color="danger"
            iconType="trash"
            onClick={() => setShowDeleteConfirm(true)}
          >
            Delete
          </EuiButtonEmpty>,
        ]}
        tabs={tabs}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route path="/" element={<DataSourceOverviewTab />} />
          <Route path="/raw-data" element={<DataSourceRawData />} />
          {CustomTabRoutes}
        </Routes>
      </EuiPageTemplate.Section>

      {showDeleteConfirm && (
        <EuiConfirmModal
          title={`Delete "${dataSourceName}"?`}
          onCancel={() => setShowDeleteConfirm(false)}
          onConfirm={handleDelete}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deleteDataSource.isLoading}
        >
          <p>
            This will permanently remove the data source. This action cannot be
            undone.
          </p>
        </EuiConfirmModal>
      )}

      {isEditModalOpen && data && (
        <DataSourceFormModal
          onClose={() => {
            setIsEditModalOpen(false);
            setEditError(null);
          }}
          onSubmit={handleEditSubmit}
          initialData={buildEditFormData(data)}
          isEdit
          isSubmitting={applyDataSource.isLoading}
          submitError={editError}
        />
      )}
    </EuiPageTemplate>
  );
};

export default DataSourceInstance;
