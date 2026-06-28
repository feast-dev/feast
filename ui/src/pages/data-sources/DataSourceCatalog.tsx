import React, { useState } from "react";
import {
  EuiFlexGroup,
  EuiFlexItem,
  EuiPanel,
  EuiText,
  EuiTitle,
  EuiSpacer,
  EuiButton,
  EuiBadge,
} from "@elastic/eui";
import { feast } from "../../protos";
import {
  BigQueryIcon,
  SnowflakeIcon,
  RedshiftIcon,
  KafkaIcon,
  SparkIcon,
  FileIcon,
  RequestSourceIcon,
  PushSourceIcon,
  KinesisIcon,
  TrinoIcon,
  AthenaIcon,
  CustomSourceIcon,
  RayIcon,
  PostgresIcon,
  MongoDBIcon,
  SqlServerIcon,
  OracleIcon,
  CouchbaseIcon,
  ClickHouseIcon,
} from "../../graphics/data-source-icons";

interface DataSourceTypeInfo {
  id: string;
  sourceType: string;
  name: string;
  description: string;
  icon: React.FC<React.SVGProps<SVGSVGElement>>;
  category: "batch" | "stream" | "on-demand";
  color: string;
  contrib?: boolean;
}

const DATA_SOURCE_TYPES: DataSourceTypeInfo[] = [
  {
    id: "bigquery",
    sourceType: String(feast.core.DataSource.SourceType.BATCH_BIGQUERY),
    name: "BigQuery",
    description:
      "Google Cloud's serverless data warehouse. Ideal for large-scale analytics and ML feature computation.",
    icon: BigQueryIcon,
    category: "batch",
    color: "#4285F4",
  },
  {
    id: "snowflake",
    sourceType: String(feast.core.DataSource.SourceType.BATCH_SNOWFLAKE),
    name: "Snowflake",
    description:
      "Cloud-native data platform with elastic scaling. Connect to your Snowflake tables for feature engineering.",
    icon: SnowflakeIcon,
    category: "batch",
    color: "#29B5E8",
  },
  {
    id: "redshift",
    sourceType: String(feast.core.DataSource.SourceType.BATCH_REDSHIFT),
    name: "Redshift",
    description:
      "AWS fully managed data warehouse. Pull features from your Redshift clusters with fast parallel queries.",
    icon: RedshiftIcon,
    category: "batch",
    color: "#205B97",
  },
  {
    id: "spark",
    sourceType: String(feast.core.DataSource.SourceType.BATCH_SPARK),
    name: "Spark",
    description:
      "Apache Spark data source for distributed processing. Access tables via Spark catalog or direct file paths.",
    icon: SparkIcon,
    category: "batch",
    color: "#E25A1C",
  },
  {
    id: "file",
    sourceType: String(feast.core.DataSource.SourceType.BATCH_FILE),
    name: "File (Parquet / CSV)",
    description:
      "Read features from Parquet or CSV files stored in S3, GCS, HDFS, or local filesystem.",
    icon: FileIcon,
    category: "batch",
    color: "#4CAF50",
  },
  {
    id: "trino",
    sourceType: String(feast.core.DataSource.SourceType.BATCH_TRINO),
    name: "Trino",
    description:
      "Distributed SQL query engine for big data analytics. Query data across heterogeneous sources via Trino catalog.",
    icon: TrinoIcon,
    category: "batch",
    color: "#DD00A1",
  },
  {
    id: "athena",
    sourceType: String(feast.core.DataSource.SourceType.BATCH_ATHENA),
    name: "AWS Athena",
    description:
      "Serverless interactive query service on AWS. Run SQL queries directly against data in S3 without infrastructure.",
    icon: AthenaIcon,
    category: "batch",
    color: "#8C4FFF",
  },
  {
    id: "kafka",
    sourceType: String(feast.core.DataSource.SourceType.STREAM_KAFKA),
    name: "Kafka",
    description:
      "Real-time event streaming platform. Ingest features from Kafka topics for low-latency serving.",
    icon: KafkaIcon,
    category: "stream",
    color: "#231F20",
  },
  {
    id: "kinesis",
    sourceType: String(feast.core.DataSource.SourceType.STREAM_KINESIS),
    name: "AWS Kinesis",
    description:
      "Managed real-time data streaming on AWS. Capture and process streaming data at scale for real-time features.",
    icon: KinesisIcon,
    category: "stream",
    color: "#FF9900",
  },
  {
    id: "request-source",
    sourceType: String(feast.core.DataSource.SourceType.REQUEST_SOURCE),
    name: "Request Source",
    description:
      "Features provided at request time by the caller. No external storage needed — values come from the client.",
    icon: RequestSourceIcon,
    category: "on-demand",
    color: "#7B61FF",
  },
  {
    id: "push-source",
    sourceType: String(feast.core.DataSource.SourceType.PUSH_SOURCE),
    name: "Push Source",
    description:
      "Push-based ingestion source. Clients push feature values directly to the online/offline store.",
    icon: PushSourceIcon,
    category: "on-demand",
    color: "#FF6B35",
  },
  {
    id: "ray",
    sourceType: "RAY_SOURCE",
    name: "Ray",
    description:
      "Multi-format data source powered by Ray. Read images, HuggingFace datasets, Parquet, CSV, MongoDB, and more via Ray Data.",
    icon: RayIcon,
    category: "batch",
    color: "#00A2E8",
    contrib: true,
  },
  {
    id: "postgres",
    sourceType: "POSTGRES_SOURCE",
    name: "PostgreSQL",
    description:
      "Open-source relational database. Query feature data from PostgreSQL tables with full SQL support.",
    icon: PostgresIcon,
    category: "batch",
    color: "#336791",
    contrib: true,
  },
  {
    id: "mongodb",
    sourceType: "MONGODB_SOURCE",
    name: "MongoDB",
    description:
      "Document-oriented NoSQL database. Access feature data stored in MongoDB collections.",
    icon: MongoDBIcon,
    category: "batch",
    color: "#00684A",
    contrib: true,
  },
  {
    id: "clickhouse",
    sourceType: "CLICKHOUSE_SOURCE",
    name: "ClickHouse",
    description:
      "Column-oriented OLAP database for real-time analytics. High-performance queries for feature retrieval.",
    icon: ClickHouseIcon,
    category: "batch",
    color: "#FFCC00",
    contrib: true,
  },
  {
    id: "mssql",
    sourceType: "MSSQL_SOURCE",
    name: "SQL Server",
    description:
      "Microsoft SQL Server data source. Connect to MSSQL databases for enterprise feature data.",
    icon: SqlServerIcon,
    category: "batch",
    color: "#CC2927",
    contrib: true,
  },
  {
    id: "oracle",
    sourceType: "ORACLE_SOURCE",
    name: "Oracle",
    description:
      "Oracle Database data source. Pull features from Oracle tables and views for enterprise workloads.",
    icon: OracleIcon,
    category: "batch",
    color: "#F80000",
    contrib: true,
  },
  {
    id: "couchbase",
    sourceType: "COUCHBASE_SOURCE",
    name: "Couchbase",
    description:
      "Couchbase Columnar analytics source. Run SQL++ queries across distributed data in Couchbase.",
    icon: CouchbaseIcon,
    category: "batch",
    color: "#EA2328",
    contrib: true,
  },
  {
    id: "custom-source",
    sourceType: String(feast.core.DataSource.SourceType.CUSTOM_SOURCE),
    name: "Custom Source",
    description:
      "Plugin-based data source for custom integrations. Extend Feast with your own data source implementation.",
    icon: CustomSourceIcon,
    category: "batch",
    color: "#607D8B",
  },
];

const CATEGORY_LABELS: Record<string, { label: string; color: string }> = {
  batch: { label: "Batch", color: "primary" },
  stream: { label: "Streaming", color: "accent" },
  "on-demand": { label: "On-Demand", color: "warning" },
};

interface DataSourceCatalogProps {
  onSelectType: (sourceType: string) => void;
}

const SourceCard: React.FC<{
  dsType: DataSourceTypeInfo;
  isHovered: boolean;
  onHover: (id: string | null) => void;
  onSelect: (sourceType: string) => void;
}> = ({ dsType, isHovered, onHover, onSelect }) => {
  const categoryInfo = CATEGORY_LABELS[dsType.category];

  return (
    <EuiFlexItem grow={false} style={{ width: 320, minWidth: 280 }}>
      <EuiPanel
        hasBorder
        hasShadow={isHovered}
        paddingSize="l"
        onMouseEnter={() => onHover(dsType.id)}
        onMouseLeave={() => onHover(null)}
        style={{
          height: "100%",
          display: "flex",
          flexDirection: "column",
          transition: "all 0.2s ease",
          transform: isHovered ? "translateY(-2px)" : "none",
          borderTop: `3px solid ${dsType.color}`,
          cursor: "pointer",
        }}
        onClick={() => onSelect(dsType.sourceType)}
      >
        <EuiFlexGroup alignItems="center" gutterSize="m" responsive={false}>
          <EuiFlexItem grow={false}>
            <div
              style={{
                width: 48,
                height: 48,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                borderRadius: 8,
                backgroundColor: `${dsType.color}10`,
              }}
            >
              <dsType.icon width={32} height={32} />
            </div>
          </EuiFlexItem>
          <EuiFlexItem>
            <EuiTitle size="xs">
              <h3 style={{ margin: 0 }}>{dsType.name}</h3>
            </EuiTitle>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiBadge color={categoryInfo.color as any}>
              {categoryInfo.label}
            </EuiBadge>
          </EuiFlexItem>
        </EuiFlexGroup>

        <EuiSpacer size="m" />

        <EuiText size="s" color="subdued" style={{ flex: 1 }}>
          <p style={{ margin: 0 }}>{dsType.description}</p>
        </EuiText>

        <EuiSpacer size="m" />

        <EuiButton
          fullWidth
          color="primary"
          fill={isHovered}
          onClick={(e: React.MouseEvent) => {
            e.stopPropagation();
            onSelect(dsType.sourceType);
          }}
          iconType="plusInCircle"
          size="s"
        >
          Create Connection
        </EuiButton>
      </EuiPanel>
    </EuiFlexItem>
  );
};

const DataSourceCatalog: React.FC<DataSourceCatalogProps> = ({
  onSelectType,
}) => {
  const [hoveredId, setHoveredId] = useState<string | null>(null);

  return (
    <div>
      <EuiText>
        <p style={{ color: "#69707D", maxWidth: 640 }}>
          Choose a data source type to create a new connection. Each type has
          its own configuration tailored to the underlying storage system.
        </p>
      </EuiText>
      <EuiSpacer size="l" />

      <EuiFlexGroup wrap responsive gutterSize="l">
        {DATA_SOURCE_TYPES.map((dsType) => (
          <SourceCard
            key={dsType.id}
            dsType={dsType}
            isHovered={hoveredId === dsType.id}
            onHover={setHoveredId}
            onSelect={onSelectType}
          />
        ))}
      </EuiFlexGroup>
    </div>
  );
};

export default DataSourceCatalog;
export { DATA_SOURCE_TYPES };
export type { DataSourceTypeInfo };
