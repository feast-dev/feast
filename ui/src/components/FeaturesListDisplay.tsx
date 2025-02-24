import { EuiBasicTable } from "@elastic/eui";
import EuiCustomLink from "./EuiCustomLink";
import { feast } from "../protos";

interface FeaturesListProps {
  projectName: string;
  featureViewName: string;
  features: feast.core.IFeatureSpecV2[];
  link: boolean;
}

const FeaturesList = ({
  projectName,
  featureViewName,
  features,
  link,
}: FeaturesListProps) => {
  let columns: { name: string; render?: any; field: any }[] = [
    {
      name: "Name",
      field: "name",
      render: (item: string) => (
        <EuiCustomLink
          to={`${process.env.PUBLIC_URL || ""}/p/${projectName}/feature-view/${featureViewName}/feature/${item}`}
        >
          {item}
        </EuiCustomLink>
      ),
    },
    {
      name: "Value Type",
      field: "valueType",
      render: (valueType: feast.types.ValueType.Enum) => {
        return feast.types.ValueType.Enum[valueType];
      },
    },
  ];

  if (!link) {
    columns[0].render = undefined;
  }

  const getRowProps = (item: feast.core.IFeatureSpecV2) => {
    return {
      "data-test-subj": `row-${item.name}`,
    };
  };

  return (
    <EuiBasicTable columns={columns} items={features} rowProps={getRowProps} />
  );
};

export default FeaturesList;
