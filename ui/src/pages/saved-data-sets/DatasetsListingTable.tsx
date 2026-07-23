import React from "react";
import { EuiBasicTable, EuiBadge } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";

interface DatasetsListingTableProps {
  datasets: any[];
}

function detectStorageType(dataset: any): string {
  const storage = dataset?.spec?.storage;
  if (!storage) return "—";
  if (storage.fileStorage) return "File";
  if (storage.bigqueryStorage) return "BigQuery";
  if (storage.snowflakeStorage) return "Snowflake";
  if (storage.redshiftStorage) return "Redshift";
  if (storage.sparkStorage) return "Spark";
  return "—";
}

const DatasetsListingTable = ({ datasets }: DatasetsListingTableProps) => {
  const { projectName } = useParams();

  const columns = [
    {
      name: "Name",
      field: "spec.name",
      sortable: true,
      render: (name: string, item: any) => {
        const itemProject = item.project || item.spec?.project || projectName;
        return (
          <EuiCustomLink to={`/p/${itemProject}/data-set/${name}`}>
            <strong>{name}</strong>
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Namespace",
      render: (item: any) => {
        const ns = item.spec?.namespace;
        return ns ? (
          <EuiBadge color="primary">{ns}</EuiBadge>
        ) : (
          <span style={{ color: "#98A2B3" }}>—</span>
        );
      },
      width: "140px",
    },
    {
      name: "Collection",
      render: (item: any) => {
        const col = item.spec?.collection;
        return col ? (
          <EuiBadge color="accent">{col}</EuiBadge>
        ) : (
          <span style={{ color: "#98A2B3" }}>—</span>
        );
      },
      width: "140px",
    },
    {
      name: "Description",
      render: (item: any) => {
        const desc = item.spec?.description;
        return desc ? (
          <span style={{ fontSize: 13 }}>
            {desc.length > 50 ? desc.slice(0, 50) + "…" : desc}
          </span>
        ) : (
          <span style={{ color: "#98A2B3" }}>—</span>
        );
      },
      width: "200px",
    },
    {
      name: "Features",
      render: (item: any) => (item.spec?.features || []).length,
      width: "90px",
    },
    {
      name: "Retrieval Keys",
      render: (item: any) =>
        (item.spec?.joinKeys || item.spec?.join_keys || []).length,
      width: "90px",
    },
    {
      name: "Storage",
      render: (item: any) => (
        <EuiBadge color="hollow">{detectStorageType(item)}</EuiBadge>
      ),
      width: "120px",
    },
    {
      name: "Feature Service",
      render: (item: any) =>
        item.spec?.featureServiceName || item.spec?.feature_service_name || "—",
    },
    {
      name: "Created",
      render: (item: any) => {
        const ts = item.meta?.createdTimestamp || item.meta?.created_timestamp;
        if (!ts) return "—";
        try {
          return new Date(ts).toLocaleDateString("en-US", {
            month: "short",
            day: "numeric",
            year: "numeric",
          });
        } catch {
          return "—";
        }
      },
      width: "140px",
    },
  ];

  const getRowProps = (item: any) => {
    return {
      "data-test-subj": `row-${item.spec?.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={datasets} rowProps={getRowProps} />
  );
};

export default DatasetsListingTable;
