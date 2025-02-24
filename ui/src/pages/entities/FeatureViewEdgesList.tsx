import React from "react";
import { EuiBasicTable, EuiLoadingSpinner } from "@elastic/eui";
import EuiCustomLink from "../../components/EuiCustomLink";
import { useParams } from "react-router-dom";
import useLoadRelationshipData from "../../queries/useLoadRelationshipsData";
import { EntityRelation } from "../../parsers/parseEntityRelationships";
import { FEAST_FCO_TYPES } from "../../parsers/types";

interface FeatureViewEdgesListInterace {
  fvNames: string[];
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

const FeatureViewEdgesList = ({ fvNames }: FeatureViewEdgesListInterace) => {
  const { projectName } = useParams();

  const { isLoading, data } = useGetFSConsumersOfFV(fvNames);

  const columns = [
    {
      name: "Name",
      field: "",
      render: ({ name }: { name: string }) => {
        return (
          <EuiCustomLink
            to={`${process.env.PUBLIC_URL || ""}/p/${projectName}/feature-view/${name}`}
          >
            {name}
          </EuiCustomLink>
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
    <EuiBasicTable columns={columns} items={fvNames.map(name => ({ name }))} rowProps={getRowProps} />
  );
};

export default FeatureViewEdgesList;
