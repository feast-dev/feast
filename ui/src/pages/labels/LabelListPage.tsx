import React, { useState } from "react";
import {
  EuiBasicTable,
  EuiTableFieldDataColumnType,
  EuiTableComputedColumnType,
  EuiFieldSearch,
  EuiPageTemplate,
  CriteriaWithPagination,
  Pagination,
  EuiText,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
} from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import ExportButton from "../../components/ExportButton";
import { useParams } from "react-router-dom";
import { LabelViewIcon } from "../../graphics/LabelViewIcon";
import useResourceQuery, {
  labelsListPath,
} from "../../queries/useResourceQuery";

interface Label {
  name: string;
  featureView: string;
  type: string;
  project?: string;
}

type LabelColumn =
  | EuiTableFieldDataColumnType<Label>
  | EuiTableComputedColumnType<Label>;

const LabelListPage = () => {
  const { projectName } = useParams();
  const {
    data: labels,
    isLoading,
    isError,
  } = useResourceQuery<any[]>({
    resourceType: "labels-list",
    project: projectName,
    restPath: labelsListPath(projectName),
    restSelect: (d) => d.labels,
  });

  const [searchText, setSearchText] = useState("");
  const [sortField, setSortField] = useState<keyof Label>("name");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(100);

  const enrichedLabels: Label[] = (labels || []).map((label: any) => ({
    ...label,
  }));

  const filteredLabels = enrichedLabels.filter((label) =>
    label.name.toLowerCase().includes(searchText.toLowerCase()),
  );

  const sortedLabels = [...filteredLabels].sort((a, b) => {
    const valueA = String(a[sortField] || "").toLowerCase();
    const valueB = String(b[sortField] || "").toLowerCase();
    return sortDirection === "asc"
      ? valueA.localeCompare(valueB)
      : valueB.localeCompare(valueA);
  });

  const paginatedLabels = sortedLabels.slice(
    pageIndex * pageSize,
    (pageIndex + 1) * pageSize,
  );

  const columns: LabelColumn[] = [
    {
      name: "Label Name",
      field: "name",
      sortable: true,
      render: (name: string, label: Label) => {
        const itemProject = label.project || projectName;
        return (
          <EuiCustomLink
            to={`/p/${itemProject}/label-view/${label.featureView}/label/${name}`}
          >
            {name}
          </EuiCustomLink>
        );
      },
    },
    {
      name: "Label View",
      field: "featureView",
      sortable: true,
      render: (featureView: string, label: Label) => {
        const itemProject = label.project || projectName;
        return (
          <EuiCustomLink to={`/p/${itemProject}/label-view/${featureView}`}>
            {featureView}
          </EuiCustomLink>
        );
      },
    },
    { name: "Type", field: "type", sortable: true },
  ];

  if (projectName === "all") {
    columns.splice(1, 0, {
      name: "Project",
      field: "project",
      sortable: true,
      render: (project: string) => {
        return <span>{project || "Unknown"}</span>;
      },
    });
  }

  const onTableChange = ({ page, sort }: CriteriaWithPagination<Label>) => {
    if (sort) {
      setSortField(sort.field as keyof Label);
      setSortDirection(sort.direction);
    }
    if (page) {
      setPageIndex(page.index);
      setPageSize(page.size);
    }
  };

  const getRowProps = (label: Label) => ({
    "data-test-subj": `row-${label.name}`,
  });

  const pagination: Pagination = {
    pageIndex,
    pageSize,
    totalItemCount: sortedLabels.length,
    pageSizeOptions: [20, 50, 100],
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={LabelViewIcon}
        pageTitle="Label List"
        rightSideItems={[
          <ExportButton data={filteredLabels} fileName="labels" />,
        ]}
      />
      <EuiPageTemplate.Section>
        {isLoading ? (
          <p>Loading...</p>
        ) : isError ? (
          <p>We encountered an error while loading.</p>
        ) : (
          <>
            <EuiFlexGroup>
              <EuiFlexItem>
                <EuiFieldSearch
                  placeholder="Search labels"
                  value={searchText}
                  onChange={(e) => setSearchText(e.target.value)}
                  fullWidth
                />
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
            <EuiText size="s" color="subdued">
              <p>
                Labels are mutable annotations from Label Views — targets,
                ground truth, or human feedback used for training and
                evaluation.
              </p>
            </EuiText>
            <EuiSpacer size="m" />
            <EuiBasicTable
              columns={columns}
              items={paginatedLabels}
              rowProps={getRowProps}
              sorting={{
                sort: { field: sortField, direction: sortDirection },
              }}
              onChange={onTableChange}
              pagination={pagination}
            />
          </>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default LabelListPage;
