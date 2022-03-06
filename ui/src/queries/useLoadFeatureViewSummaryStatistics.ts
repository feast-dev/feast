import { useQuery } from "react-query";
import { useParams } from "react-router-dom";
import {
  featureViewSummaryStatisticsSchema,
  FeatureViewSummaryStatisticsType,
} from "../parsers/featureViewSummaryStatistics";

const useLoadFeatureViewSummaryStatistics = (featureViewName: string) => {
  const { projectName } = useParams();

  const queryKey = `featureViewSummaryStatistics:${featureViewName}`;
  const url = `/metadata/${projectName}/featureView/${featureViewName}.json`;

  return useQuery(
    queryKey,
    () => {
      return fetch(url, {
        headers: {
          "Content-Type": "application/json",
        },
      })
        .then((res) => {
          return res.json();
        })
        .then<FeatureViewSummaryStatisticsType>((json) => {
          const summary = featureViewSummaryStatisticsSchema.parse(json);

          return summary;
        });
    },
    {
      staleTime: 15 * 60 * 1000, // Given that we are reading from a registry dump, this seems reasonable for now.
    }
  );
};

export default useLoadFeatureViewSummaryStatistics;
