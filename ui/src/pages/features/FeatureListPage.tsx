import React, { useState, useContext } from "react";
import {
  EuiBasicTable,
  EuiTableFieldDataColumnType,
  EuiTableComputedColumnType,
  EuiFieldSearch,
  EuiPageTemplate,
  CriteriaWithPagination,
  Pagination,
  EuiToolTip,
  EuiIcon,
  EuiText,
  EuiSpacer,
  EuiSelect,
  EuiFlexGroup,
  EuiFlexItem,
  EuiFormRow,
} from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import ExportButton from "../../components/ExportButton";
import { useParams } from "react-router-dom";
import useLoadRegistry from "../../queries/useLoadRegistry";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import { FeatureIcon } from "../../graphics/FeatureIcon";
import { FEAST_FCO_TYPES } from "../../parsers/types";
import { getEntityPermissions, formatPermissions, filterPermissionsByAction } from "../../utils/permissionUtils";

interface Feature {
  name: string;
  featureView: string;
  type: string;
  permissions?: any[];
}

type FeatureColumn =
  | EuiTableFieldDataColumnType<Feature>
  | EuiTableComputedColumnType<Feature>;

const FeatureListPage = () => {
  const { projectName } = useParams();
  const registryUrl = useContext(RegistryPathContext);
  const { data, isLoading, isError } = useLoadRegistry(registryUrl);
  const [searchText, setSearchText] = useState("");
  const [selectedPermissionAction, setSelectedPermissionAction] = useState("");

  const [sortField, setSortField] = useState<keyof Feature>("name");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");

  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(100);

  const featuresWithPermissions: Feature[] = (data?.allFeatures || []).map(feature => {
    return {
      ...feature,
      permissions: getEntityPermissions(
        selectedPermissionAction 
          ? filterPermissionsByAction(data?.permissions, selectedPermissionAction)
          : data?.permissions,
        FEAST_FCO_TYPES.featureView,
        feature.featureView
      )
    };
  });

  const features: Feature[] = featuresWithPermissions;

  const filteredFeatures = features.filter((feature) =>
    feature.name.toLowerCase().includes(searchText.toLowerCase()),
  );

  const sortedFeatures = [...filteredFeatures].sort((a, b) => {
    const valueA = String(a[sortField] || "").toLowerCase();
    const valueB = String(b[sortField] || "").toLowerCase();
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
    {
      name: "Permissions",
      field: "permissions",
      sortable: false,
      render: (permissions: any[], feature: Feature) => {
        const hasPermissions = permissions && permissions.length > 0;
        return hasPermissions ? (
          <EuiToolTip
            position="top"
            content={<pre style={{ margin: 0 }}>{formatPermissions(permissions)}</pre>}
          >
            <div style={{ display: "flex", alignItems: "center" }}>
              <EuiIcon type="lock" color="#5a7be0" />
              <EuiText size="xs" style={{ marginLeft: "4px" }}>
                {permissions.length} permission{permissions.length !== 1 ? "s" : ""}
              </EuiText>
            </div>
          </EuiToolTip>
        ) : (
          <EuiText size="xs" color="subdued">None</EuiText>
        );
      },
    },
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
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureIcon}
        pageTitle="Feature List"
        rightSideItems={[
          <ExportButton data={filteredFeatures} fileName="features" />,
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
                  placeholder="Search features"
                  value={searchText}
                  onChange={(e) => setSearchText(e.target.value)}
                  fullWidth
                />
              </EuiFlexItem>
              <EuiFlexItem grow={false} style={{ width: 300 }}>
                <EuiFormRow label="Filter by permission action">
                  <EuiSelect
                    options={[
                      { value: "", text: "All" },
                      { value: "CREATE", text: "CREATE" },
                      { value: "DESCRIBE", text: "DESCRIBE" },
                      { value: "UPDATE", text: "UPDATE" },
                      { value: "DELETE", text: "DELETE" },
                      { value: "READ_ONLINE", text: "READ_ONLINE" },
                      { value: "READ_OFFLINE", text: "READ_OFFLINE" },
                      { value: "WRITE_ONLINE", text: "WRITE_ONLINE" },
                      { value: "WRITE_OFFLINE", text: "WRITE_OFFLINE" },
                    ]}
                    value={selectedPermissionAction}
                    onChange={(e) => setSelectedPermissionAction(e.target.value)}
                    aria-label="Filter by permission action"
                  />
                </EuiFormRow>
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
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
          </>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default FeatureListPage;
