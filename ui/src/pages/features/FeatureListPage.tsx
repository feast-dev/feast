import React, { useState, useContext } from "react";
import {
  EuiBasicTable,
  EuiTableFieldDataColumnType,
  EuiTableComputedColumnType,
  EuiFieldSearch,
  EuiPageTemplate,
  CriteriaWithPagination,
  Pagination,
} from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";

interface Feature {
  name: string;
  featureView: string;
  type: string;
}

type FeatureColumn =
  | EuiTableFieldDataColumnType<Feature>
  | EuiTableComputedColumnType<Feature>;

const FeatureListPage = () => {
  const { projectName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const { data, isLoading, isError } = useLoadRegistry(registryUrl);
  const [searchText, setSearchText] = useState("");

  const [sortField, setSortField] = useState<keyof Feature>("name");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");

  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(100);

  if (isLoading) return <p>Loading...</p>;
  if (isError) return <p>Error loading features.</p>;

  const features: Feature[] = data?.allFeatures || [];

  const filteredFeatures = features.filter((feature) =>
    feature.name.toLowerCase().includes(searchText.toLowerCase()),
  );

  const sortedFeatures = [...filteredFeatures].sort((a, b) => {
    const valueA = a[sortField].toLowerCase();
    const valueB = b[sortField].toLowerCase();
    return sortDirection === "asc"
      ? valueA.localeCompare(valueB)
      : valueB.localeCompare(valueA);
  });

  const paginatedFeatures = sortedFeatures.slice(
    pageIndex * pageSize,
    (pageIndex + 1) * pageSize,
  );

  const columns: FeatureColumn[] = [
    {
      name: "Feature Name",
      field: "name",
      sortable: true,
      render: (name: string, feature: Feature) => (
        <EuiCustomLink
          to={`/p/${projectName}/feature-view/${feature.featureView}/feature/${name}`}
        >
          {name}
        </EuiCustomLink>
      ),
    },
    {
      name: "Feature View",
      field: "featureView",
      sortable: true,
      render: (featureView: string) => (
        <EuiCustomLink to={`/p/${projectName}/feature-view/${featureView}`}>
          {featureView}
        </EuiCustomLink>
      ),
    },
    { name: "Type", field: "type", sortable: true },
  ];

  const onTableChange = ({ page, sort }: CriteriaWithPagination<Feature>) => {
    if (sort) {
      setSortField(sort.field as keyof Feature);
      setSortDirection(sort.direction);
    }
    if (page) {
      setPageIndex(page.index);
      setPageSize(page.size);
    }
  };

  const getRowProps = (feature: Feature) => ({
    "data-test-subj": `row-${feature.name}`,
  });

  const pagination: Pagination = {
    pageIndex,
    pageSize,
    totalItemCount: sortedFeatures.length,
    pageSizeOptions: [20, 50, 100],
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header pageTitle="Feature List" />
      <EuiPageTemplate.Section>
        <EuiFieldSearch
          placeholder="Search features"
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          fullWidth
        />
        <EuiBasicTable
          columns={columns}
          items={paginatedFeatures}
          rowProps={getRowProps}
          sorting={{
            sort: { field: sortField, direction: sortDirection },
          }}
          onChange={onTableChange}
          pagination={pagination}
        />
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default FeatureListPage;
