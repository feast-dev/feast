import React from "react";
import { EuiInMemoryTable, EuiBadge, EuiText, EuiToolTip } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";
import {
  getDataSourceTypeMeta,
  getConnectionTarget,
  truncateMiddle,
  getRelativeTime,
  formatAbsoluteDate,
  CATEGORY_META,
} from "./dataSourceTypeUtils";

interface DataSourcesListingTableProps {
  dataSources: any[];
}

function createdTsNum(item: any): number {
  const ts =
    item.meta?.createdTimestamp ||
    item.meta?.created_timestamp ||
    item.spec?.meta?.createdTimestamp;
  if (!ts) return 0;
  const n = new Date(ts).getTime();
  return Number.isNaN(n) ? 0 : n;
}

const DataSourcesListingTable = ({
  dataSources,
}: DataSourcesListingTableProps) => {
  const { projectName } = useParams();

  const columns: any[] = [
    {
      name: "Name",
      field: "name",
      sortable: (item: any) => item.name || item.spec?.name || "",
      render: (_name: string, item: any) => {
        const ds = item.spec || item;
        const name = ds.name || item.name;
        const itemProject = item.project || ds.project || projectName;
        const description = ds.description;
        return (
          <div>
            <EuiCustomLink to={`/p/${itemProject}/data-source/${name}`}>
              <strong>{name}</strong>
            </EuiCustomLink>
            {description && (
              <EuiText size="xs" color="subdued">
                <span
                  style={{
                    display: "-webkit-box",
                    WebkitLineClamp: 1,
                    WebkitBoxOrient: "vertical",
                    overflow: "hidden",
                  }}
                >
                  {description}
                </span>
              </EuiText>
            )}
          </div>
        );
      },
    },
    {
      name: "Type",
      field: "type",
      sortable: (item: any) => getDataSourceTypeMeta(item).label,
      render: (_type: any, item: any) => {
        const meta = getDataSourceTypeMeta(item);
        const Icon = meta.icon;
        return (
          <span
            style={{ display: "inline-flex", alignItems: "center", gap: 6 }}
          >
            <Icon width={16} height={16} />
            {meta.label}
          </span>
        );
      },
    },
    {
      name: "Category",
      sortable: (item: any) => getDataSourceTypeMeta(item).category,
      render: (item: any) => {
        const meta = getDataSourceTypeMeta(item);
        const cat = CATEGORY_META[meta.category];
        return <EuiBadge color={cat.color as any}>{cat.label}</EuiBadge>;
      },
      width: "120px",
    },
    {
      name: "Connection",
      render: (item: any) => {
        const target = getConnectionTarget(item);
        if (!target) return <span style={{ color: "#98A2B3" }}>—</span>;
        return (
          <EuiToolTip content={`${target.label}: ${target.value}`}>
            <EuiText size="xs">
              <span style={{ color: "#69707D" }}>{target.label}:</span>{" "}
              <code style={{ fontSize: 11 }}>
                {truncateMiddle(target.value, 36)}
              </code>
            </EuiText>
          </EuiToolTip>
        );
      },
    },
    {
      name: "Owner",
      sortable: (item: any) => (item.spec || item).owner || "",
      render: (item: any) => {
        const owner = (item.spec || item).owner;
        return owner ? (
          <EuiText size="xs">{owner}</EuiText>
        ) : (
          <span style={{ color: "#98A2B3" }}>—</span>
        );
      },
      width: "150px",
    },
    {
      name: "Created",
      sortable: (item: any) => createdTsNum(item),
      render: (item: any) => {
        const ts = createdTsNum(item);
        if (!ts) return <span style={{ color: "#98A2B3" }}>—</span>;
        return (
          <EuiToolTip content={formatAbsoluteDate(ts)}>
            <EuiText size="xs">{getRelativeTime(new Date(ts))}</EuiText>
          </EuiToolTip>
        );
      },
      width: "130px",
    },
  ];

  // Add Project column when viewing all projects
  if (projectName === "all") {
    columns.splice(1, 0, {
      name: "Project",
      field: "project",
      sortable: true,
      render: (project: string) => <span>{project || "Unknown"}</span>,
      width: "140px",
    });
  }

  const getRowProps = (item: any) => ({
    "data-test-subj": `row-${item.name || item.spec?.name}`,
  });

  return (
    <EuiInMemoryTable
      columns={columns}
      items={dataSources}
      rowProps={getRowProps}
      sorting={true}
      pagination={{
        initialPageSize: 10,
        pageSizeOptions: [10, 25, 50],
      }}
    />
  );
};

export default DataSourcesListingTable;
