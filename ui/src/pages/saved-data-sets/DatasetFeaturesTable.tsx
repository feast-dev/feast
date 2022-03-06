import { EuiBasicTable } from "@elastic/eui";
import React from "react";

interface DatasetFeatureEntry {
  featureName: string;
  featureViewName: string;
}

interface DatasetFeaturesTableProps {
  features: DatasetFeatureEntry[];
}

const DatasetFeaturesTable = ({ features }: DatasetFeaturesTableProps) => {
  const columns = [
    {
      name: "Feature",
      field: "featureName",
    },
    {
      name: "Sourc Feature View",
      field: "featureViewName",
    },
  ];

  return <EuiBasicTable columns={columns} items={features} />;
};

export default DatasetFeaturesTable;
