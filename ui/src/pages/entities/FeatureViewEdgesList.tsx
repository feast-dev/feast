import React from "react";
import { EuiBasicTable, EuiBadge, EuiLoadingSpinner } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";
import useLoadRelationshipData from "../../queries/useLoadRelationshipsData";
import { EntityRelation } from "../../parsers/parseEntityRelationships";
import { FEAST_FCO_TYPES } from "../../parsers/types";

interface FeatureViewEdgesListInterace {
  fvNames: string[];
  viewTypes?: Record<string, string>;
}

const whereFSconsumesThisFv = (fvName: string) => {
  return (r: EntityRelation) => {
    return (
      r.source.name === fvName &&
      r.target.type === FEAST_FCO_TYPES.featureService
    );
  };
};

const useGetFSConsumersOfFV = (fvList: string[]) => {
  const relationshipQuery = useLoadRelationshipData();

  const data = relationshipQuery.data
    ? fvList.reduce((memo: Record<string, string[]>, fvName) => {
        if (relationshipQuery.data) {
          memo[fvName] = relationshipQuery.data
            .filter(whereFSconsumesThisFv(fvName))
            .map((fs) => {
              return fs.target.name;
            });
        }

        return memo;
      }, {})
    : undefined;

  return {
    ...relationshipQuery,
    data,
  };
};

const FeatureViewEdgesList = ({
  fvNames,
  viewTypes,
}: FeatureViewEdgesListInterace) => {
  const { projectName } = useParams();

  const { isLoading, data } = useGetFSConsumersOfFV(fvNames);

  const columns = [
    {
      name: "Name",
      field: "",
      render: ({ name }: { name: string }) => {
        const isLabelView = viewTypes?.[name] === "labelView";
        const path = isLabelView
          ? `/p/${projectName}/label-view/${name}`
          : `/p/${projectName}/feature-view/${name}`;
        return (
          <React.Fragment>
            <EuiCustomLink to={path}>{name}</EuiCustomLink>
            {isLabelView && (
              <>
                {" "}
                <EuiBadge color="#e6570e">label view</EuiBadge>
              </>
            )}
          </React.Fragment>
        );
      },
    },
    {
      name: "FS Consumers",
      field: "",
      render: ({ name }: { name: string }) => {
        return (
          <React.Fragment>
            {isLoading && <EuiLoadingSpinner size="s" />}
            {data && data[name].length}
          </React.Fragment>
        );
      },
    },
  ];

  const getRowProps = (item: string) => {
    return {
      "data-test-subj": `row-${item}`,
    };
  };

  return (
    <EuiBasicTable
      columns={columns}
      items={fvNames.map((name) => ({ name }))}
      rowProps={getRowProps}
    />
  );
};

export default FeatureViewEdgesList;
