import React from "react";
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

/**
 * Shared presentation metadata for Feast data source types.
 *
 * A single source of truth so the listing table, card grid, catalog browser
 * and the detail page all render the same icon / colour / category / label for
 * a given data source. Detection works from a flat `IDataSource` proto object
 * (as returned by the REST registry) by inspecting the typed options first,
 * then the `type` enum, then the contrib `dataSourceClassType`.
 */

export type DataSourceCategory = "batch" | "stream" | "on-demand";

export interface DataSourceTypeMeta {
  /** canonical, stable id used for grouping and lookups */
  id: string;
  label: string;
  category: DataSourceCategory;
  /** brand/accent colour used for borders, icon chips and badges */
  color: string;
  /** EUI SVG icon component */
  icon: React.FC<React.SVGProps<SVGSVGElement>>;
  description: string;
}

export const CATEGORY_META: Record<
  DataSourceCategory,
  { label: string; color: string; icon: string; description: string }
> = {
  batch: {
    label: "Batch",
    color: "primary",
    icon: "storage",
    description:
      "Historical data stores read in bulk for training and backfills.",
  },
  stream: {
    label: "Streaming",
    color: "accent",
    icon: "bolt",
    description: "Real-time event streams ingested for low-latency features.",
  },
  "on-demand": {
    label: "On-Demand",
    color: "warning",
    icon: "inputOutput",
    description: "Values supplied at request time — no external storage.",
  },
};

export const DATA_SOURCE_TYPE_META: Record<string, DataSourceTypeMeta> = {
  file: {
    id: "file",
    label: "File",
    category: "batch",
    color: "#4CAF50",
    icon: FileIcon,
    description:
      "Parquet or CSV files in S3, GCS, HDFS, or the local filesystem.",
  },
  bigquery: {
    id: "bigquery",
    label: "BigQuery",
    category: "batch",
    color: "#4285F4",
    icon: BigQueryIcon,
    description: "Google Cloud's serverless data warehouse.",
  },
  snowflake: {
    id: "snowflake",
    label: "Snowflake",
    category: "batch",
    color: "#29B5E8",
    icon: SnowflakeIcon,
    description: "Cloud-native data platform with elastic scaling.",
  },
  redshift: {
    id: "redshift",
    label: "Redshift",
    category: "batch",
    color: "#205B97",
    icon: RedshiftIcon,
    description: "AWS fully managed data warehouse.",
  },
  spark: {
    id: "spark",
    label: "Spark",
    category: "batch",
    color: "#E25A1C",
    icon: SparkIcon,
    description: "Apache Spark catalog tables or direct file paths.",
  },
  trino: {
    id: "trino",
    label: "Trino",
    category: "batch",
    color: "#DD00A1",
    icon: TrinoIcon,
    description: "Distributed SQL query engine for big data analytics.",
  },
  athena: {
    id: "athena",
    label: "AWS Athena",
    category: "batch",
    color: "#8C4FFF",
    icon: AthenaIcon,
    description: "Serverless interactive SQL over data in S3.",
  },
  iceberg: {
    id: "iceberg",
    label: "Iceberg / Unity Catalog",
    category: "batch",
    color: "#3B82F6",
    icon: CustomSourceIcon,
    description: "Apache Iceberg REST catalog, including Unity Catalog.",
  },
  kafka: {
    id: "kafka",
    label: "Kafka",
    category: "stream",
    color: "#231F20",
    icon: KafkaIcon,
    description: "Real-time event streaming platform.",
  },
  kinesis: {
    id: "kinesis",
    label: "AWS Kinesis",
    category: "stream",
    color: "#FF9900",
    icon: KinesisIcon,
    description: "Managed real-time data streaming on AWS.",
  },
  "request-source": {
    id: "request-source",
    label: "Request Source",
    category: "on-demand",
    color: "#7B61FF",
    icon: RequestSourceIcon,
    description: "Features provided at request time by the caller.",
  },
  "push-source": {
    id: "push-source",
    label: "Push Source",
    category: "on-demand",
    color: "#FF6B35",
    icon: PushSourceIcon,
    description: "Clients push feature values directly to the store.",
  },
  ray: {
    id: "ray",
    label: "Ray",
    category: "batch",
    color: "#00A2E8",
    icon: RayIcon,
    description: "Multi-format data source powered by Ray Data.",
  },
  postgres: {
    id: "postgres",
    label: "PostgreSQL",
    category: "batch",
    color: "#336791",
    icon: PostgresIcon,
    description: "Open-source relational database with full SQL support.",
  },
  mongodb: {
    id: "mongodb",
    label: "MongoDB",
    category: "batch",
    color: "#00684A",
    icon: MongoDBIcon,
    description: "Document-oriented NoSQL database.",
  },
  clickhouse: {
    id: "clickhouse",
    label: "ClickHouse",
    category: "batch",
    color: "#FFCC00",
    icon: ClickHouseIcon,
    description: "Column-oriented OLAP database for real-time analytics.",
  },
  mssql: {
    id: "mssql",
    label: "SQL Server",
    category: "batch",
    color: "#CC2927",
    icon: SqlServerIcon,
    description: "Microsoft SQL Server data source.",
  },
  oracle: {
    id: "oracle",
    label: "Oracle",
    category: "batch",
    color: "#F80000",
    icon: OracleIcon,
    description: "Oracle Database tables and views.",
  },
  couchbase: {
    id: "couchbase",
    label: "Couchbase",
    category: "batch",
    color: "#EA2328",
    icon: CouchbaseIcon,
    description: "Couchbase Columnar analytics (SQL++).",
  },
  custom: {
    id: "custom",
    label: "Custom Source",
    category: "batch",
    color: "#607D8B",
    icon: CustomSourceIcon,
    description: "Plugin-based data source for custom integrations.",
  },
  unknown: {
    id: "unknown",
    label: "Data Source",
    category: "batch",
    color: "#98A2B3",
    icon: CustomSourceIcon,
    description: "Feast data source.",
  },
};

const SourceType = feast.core.DataSource.SourceType;

/** Map a numeric/string proto `type` enum to a canonical id. */
function idFromEnum(type: unknown): string | undefined {
  const t = typeof type === "string" ? type : String(type);
  // Accept both the numeric enum and its string name.
  switch (t) {
    case String(SourceType.BATCH_FILE):
    case "BATCH_FILE":
      return "file";
    case String(SourceType.BATCH_BIGQUERY):
    case "BATCH_BIGQUERY":
      return "bigquery";
    case String(SourceType.BATCH_SNOWFLAKE):
    case "BATCH_SNOWFLAKE":
      return "snowflake";
    case String(SourceType.BATCH_REDSHIFT):
    case "BATCH_REDSHIFT":
      return "redshift";
    case String(SourceType.BATCH_SPARK):
    case "BATCH_SPARK":
      return "spark";
    case String(SourceType.BATCH_TRINO):
    case "BATCH_TRINO":
      return "trino";
    case String(SourceType.BATCH_ATHENA):
    case "BATCH_ATHENA":
      return "athena";
    case String(SourceType.BATCH_ICEBERG):
    case "BATCH_ICEBERG":
      return "iceberg";
    case String(SourceType.STREAM_KAFKA):
    case "STREAM_KAFKA":
      return "kafka";
    case String(SourceType.STREAM_KINESIS):
    case "STREAM_KINESIS":
      return "kinesis";
    case String(SourceType.REQUEST_SOURCE):
    case "REQUEST_SOURCE":
      return "request-source";
    case String(SourceType.PUSH_SOURCE):
    case "PUSH_SOURCE":
      return "push-source";
    default:
      return undefined;
  }
}

/** Map a contrib `dataSourceClassType` fully-qualified class to a canonical id. */
function idFromClassType(classType?: string): string | undefined {
  if (!classType) return undefined;
  const lower = classType.toLowerCase();
  if (lower.includes("iceberg")) return "iceberg";
  if (lower.includes("ray")) return "ray";
  if (lower.includes("postgres")) return "postgres";
  if (lower.includes("mongodb") || lower.includes("mongo")) return "mongodb";
  if (lower.includes("clickhouse")) return "clickhouse";
  if (lower.includes("mssql") || lower.includes("sqlserver")) return "mssql";
  if (lower.includes("oracle")) return "oracle";
  if (lower.includes("couchbase")) return "couchbase";
  return undefined;
}

/**
 * Detect the canonical data source type id from a flat `IDataSource`-shaped
 * object (may be wrapped in `.spec`).
 */
export function detectDataSourceTypeId(dataSource: any): string {
  const ds = dataSource?.spec || dataSource || {};

  // 1. Typed options take priority — they are the most reliable signal.
  if (ds.fileOptions) return "file";
  if (ds.bigqueryOptions) return "bigquery";
  if (ds.snowflakeOptions) return "snowflake";
  if (ds.redshiftOptions) return "redshift";
  if (ds.sparkOptions) return "spark";
  if (ds.trinoOptions) return "trino";
  if (ds.athenaOptions) return "athena";
  if (ds.kafkaOptions) return "kafka";
  if (ds.kinesisOptions) return "kinesis";
  if (ds.requestDataOptions) return "request-source";
  if (ds.pushOptions) return "push-source";

  // 2. Contrib / custom sources — resolve by class type.
  const byClass = idFromClassType(ds.dataSourceClassType);
  if (byClass) return byClass;

  // 3. Fall back to the numeric `type` enum.
  const byEnum = idFromEnum(ds.type);
  if (byEnum) return byEnum;

  // 4. Custom source with opaque options.
  if (ds.customOptions) return "custom";

  return "unknown";
}

export function getDataSourceTypeMeta(dataSource: any): DataSourceTypeMeta {
  const id = detectDataSourceTypeId(dataSource);
  return DATA_SOURCE_TYPE_META[id] || DATA_SOURCE_TYPE_META.unknown;
}

export interface ConnectionTarget {
  label: string;
  value: string;
}

/**
 * Extract the primary human-readable connection target (table / path / topic /
 * stream / endpoint) for a data source, for display on cards and tables.
 */
export function getConnectionTarget(
  dataSource: any,
): ConnectionTarget | undefined {
  const ds = dataSource?.spec || dataSource || {};

  if (ds.fileOptions?.uri) return { label: "File", value: ds.fileOptions.uri };
  if (ds.bigqueryOptions)
    return ds.bigqueryOptions.table
      ? { label: "Table", value: ds.bigqueryOptions.table }
      : ds.bigqueryOptions.query
        ? { label: "Query", value: ds.bigqueryOptions.query }
        : undefined;
  if (ds.snowflakeOptions?.table)
    return { label: "Table", value: ds.snowflakeOptions.table };
  if (ds.redshiftOptions?.table)
    return { label: "Table", value: ds.redshiftOptions.table };
  if (ds.sparkOptions?.table)
    return { label: "Table", value: ds.sparkOptions.table };
  if (ds.sparkOptions?.path)
    return { label: "Path", value: ds.sparkOptions.path };
  if (ds.trinoOptions?.table)
    return { label: "Table", value: ds.trinoOptions.table };
  if (ds.athenaOptions?.table)
    return { label: "Table", value: ds.athenaOptions.table };
  if (ds.kafkaOptions?.topic)
    return { label: "Topic", value: ds.kafkaOptions.topic };
  if (ds.kinesisOptions?.streamName)
    return { label: "Stream", value: ds.kinesisOptions.streamName };

  // Iceberg / contrib custom options carry a JSON configuration blob.
  if (ds.customOptions?.configuration) {
    try {
      const cfg = JSON.parse(ds.customOptions.configuration);
      if (cfg.table) return { label: "Table", value: String(cfg.table) };
      if (cfg.endpoint)
        return { label: "Endpoint", value: String(cfg.endpoint) };
    } catch {
      /* not JSON — fall through */
    }
  }
  if (ds.customOptions?.className)
    return { label: "Class", value: ds.customOptions.className };

  return undefined;
}

/** Shorten a long path/uri for compact display while keeping the tails. */
export function truncateMiddle(value: string, maxLen: number = 42): string {
  if (!value || value.length <= maxLen) return value;
  return `${value.slice(0, 18)}...${value.slice(-20)}`;
}

export function getRelativeTime(date: Date): string {
  const diffMs = Date.now() - date.getTime();
  const diffDays = Math.floor(diffMs / 86400000);
  if (Number.isNaN(diffDays)) return "";
  if (diffDays <= 0) return "Today";
  if (diffDays === 1) return "Yesterday";
  if (diffDays < 7) return `${diffDays}d ago`;
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`;
  if (diffDays < 365) return `${Math.floor(diffDays / 30)}mo ago`;
  return `${Math.floor(diffDays / 365)}y ago`;
}

/** Absolute, human-readable date (used in tooltips next to a relative time). */
export function formatAbsoluteDate(ts: any): string {
  if (!ts) return "—";
  try {
    const d = ts instanceof Date ? ts : new Date(ts);
    if (Number.isNaN(d.getTime())) return "—";
    return d.toLocaleDateString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  } catch {
    return "—";
  }
}

export interface TagBadge {
  color: string;
  label: string;
}

const TIER_COLORS: Record<string, string> = {
  gold: "#F5A700",
  silver: "#9AA5B1",
  bronze: "#CD7F32",
};

/**
 * Render well-known metadata tag keys with friendly labels and semantic
 * colours (e.g. a PII flag), falling back to a neutral `key: value` badge.
 */
export function getTagBadge(key: string, value: string): TagBadge {
  const k = key.toLowerCase();
  const v = String(value).toLowerCase();
  const fallback: TagBadge = { color: "hollow", label: `${key}: ${value}` };

  if (k === "pii") {
    return v === "true" || v === "yes"
      ? { color: "danger", label: "PII" }
      : fallback;
  }
  if (k === "tier") {
    return { color: TIER_COLORS[v] || "accent", label: `Tier: ${value}` };
  }
  if (k === "team") {
    return { color: "primary", label: `Team: ${value}` };
  }
  if (k === "latency" && (v === "realtime" || v === "real-time")) {
    return { color: "accent", label: "Real-time" };
  }
  return fallback;
}
