import { useMemo } from "react";
import { useParams } from "react-router-dom";
import { feast } from "../protos";
import useResourceQuery, {
  featureViewListPath,
  featureServiceListPath,
} from "../queries/useResourceQuery";

const buildTagCollection = <T>(
  array: T[],
  recordExtractor: (unknownFCO: T) => Record<string, string> | undefined,
): Record<string, Record<string, T[]>> => {
  const tagCollection = array.reduce(
    (memo: Record<string, Record<string, T[]>>, fco: T) => {
      const tags = recordExtractor(fco);

      if (tags) {
        Object.entries(tags).forEach(([tagKey, tagValue]) => {
          if (!memo[tagKey]) {
            memo[tagKey] = {
              [tagValue]: [fco],
            };
          } else {
            if (!memo[tagKey][tagValue]) {
              memo[tagKey][tagValue] = [fco];
            } else {
              memo[tagKey][tagValue].push(fco);
            }
          }
        });
      }

      return memo;
    },
    {},
  );

  return tagCollection;
};

const useFeatureViewTagsAggregation = () => {
  const { projectName } = useParams();
  const query = useResourceQuery<any[]>({
    resourceType: "tags-fvs",
    project: projectName,
    protoSelect: (d) => d.objects.featureViews,
    restPath: featureViewListPath(projectName),
    restSelect: (d) => d.featureViews,
  });

  const data = useMemo(() => {
    return query.data
      ? buildTagCollection<any>(query.data, (fv) => fv.spec?.tags)
      : undefined;
  }, [query.data]);

  return {
    ...query,
    data,
  };
};

const useFeatureServiceTagsAggregation = () => {
  const { projectName } = useParams();
  const query = useResourceQuery<any[]>({
    resourceType: "tags-fss",
    project: projectName,
    protoSelect: (d) => d.objects.featureServices,
    restPath: featureServiceListPath(projectName),
    restSelect: (d) => d.featureServices,
  });

  const data = useMemo(() => {
    return query.data
      ? buildTagCollection<any>(query.data, (fs) => fs.spec?.tags)
      : undefined;
  }, [query.data]);

  return {
    ...query,
    data,
  };
};

export { useFeatureViewTagsAggregation, useFeatureServiceTagsAggregation };
