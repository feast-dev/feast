import React, { useContext, useState } from "react";
import { EuiEmptyPrompt, EuiLoadingSpinner, EuiSpacer, EuiSelect, EuiFormRow, EuiFlexGroup, EuiFlexItem } from "@elastic/eui";
import useLoadRegistry from "../queries/useLoadRegistry";
import RegistryPathContext from "../contexts/RegistryPathContext";
import RegistryVisualization from "./RegistryVisualization";
import { FEAST_FCO_TYPES } from "../parsers/types";

const RegistryVisualizationTab = () => {
  const registryUrl = useContext(RegistryPathContext);
  const { isLoading, isSuccess, isError, data } = useLoadRegistry(registryUrl);
  const [selectedObjectType, setSelectedObjectType] = useState("");
  const [selectedObjectName, setSelectedObjectName] = useState("");
  
  const getObjectOptions = (objects: any, type: string) => {
    switch (type) {
      case "dataSource":
        const dataSources = new Set<string>();
        objects.featureViews?.forEach((fv: any) => {
          if (fv.spec?.batchSource?.name) dataSources.add(fv.spec.batchSource.name);
        });
        objects.streamFeatureViews?.forEach((sfv: any) => {
          if (sfv.spec?.batchSource?.name) dataSources.add(sfv.spec.batchSource.name);
          if (sfv.spec?.streamSource?.name) dataSources.add(sfv.spec.streamSource.name);
        });
        return Array.from(dataSources);
      case "entity":
        return objects.entities?.map((entity: any) => entity.spec?.name) || [];
      case "featureView":
        return [...(objects.featureViews?.map((fv: any) => fv.spec?.name) || []),
                ...(objects.onDemandFeatureViews?.map((odfv: any) => odfv.spec?.name) || []),
                ...(objects.streamFeatureViews?.map((sfv: any) => sfv.spec?.name) || [])];
      case "featureService":
        return objects.featureServices?.map((fs: any) => fs.spec?.name) || [];
      default:
        return [];
    }
  };

  return (
    <>
      {isLoading && (
        <div style={{ display: "flex", justifyContent: "center", padding: 25 }}>
          <EuiLoadingSpinner size="xl" />
        </div>
      )}
      {isError && (
        <EuiEmptyPrompt
          iconType="alert"
          color="danger"
          title={<h2>Error Loading Registry Data</h2>}
          body={
            <p>
              There was an error loading the Registry Data. Please check that{" "}
              <code>feature_store.yaml</code> file is available and well-formed.
            </p>
          }
        />
      )}
      {isSuccess && data && (
        <>
          <EuiSpacer size="l" />
          <EuiFlexGroup style={{ marginBottom: 16 }}>
            <EuiFlexItem grow={false} style={{ width: 200 }}>
              <EuiFormRow label="Filter by type">
                <EuiSelect
                  options={[
                    { value: "", text: "All" },
                    { value: "dataSource", text: "Data Source" },
                    { value: "entity", text: "Entity" },
                    { value: "featureView", text: "Feature View" },
                    { value: "featureService", text: "Feature Service" },
                  ]}
                  value={selectedObjectType}
                  onChange={(e) => {
                    setSelectedObjectType(e.target.value);
                    setSelectedObjectName(""); // Reset name when type changes
                  }}
                  aria-label="Select object type"
                />
              </EuiFormRow>
            </EuiFlexItem>
            <EuiFlexItem grow={false} style={{ width: 300 }}>
              <EuiFormRow label="Select object">
                <EuiSelect
                  options={[
                    { value: "", text: "All" },
                    ...getObjectOptions(data.objects, selectedObjectType).map((name) => ({
                      value: name,
                      text: name,
                    })),
                  ]}
                  value={selectedObjectName}
                  onChange={(e) => setSelectedObjectName(e.target.value)}
                  aria-label="Select object"
                  disabled={selectedObjectType === ""}
                />
              </EuiFormRow>
            </EuiFlexItem>
          </EuiFlexGroup>
          <RegistryVisualization
            registryData={data.objects}
            relationships={data.relationships}
            indirectRelationships={data.indirectRelationships}
            filterNode={
              selectedObjectType && selectedObjectName
                ? {
                    type: selectedObjectType as FEAST_FCO_TYPES,
                    name: selectedObjectName,
                  }
                : undefined
            }
          />
        </>
      )}
    </>
  );
};

export default RegistryVisualizationTab;
