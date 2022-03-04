import { useContext, useMemo } from "react";
import RegistryPathContext from "../contexts/RegistryPathContext";
import { FeastFeatureServiceType } from "../parsers/feastFeatureServices";
import { FeastFeatureViewType } from "../parsers/feastFeatureViews";
import useLoadRegistry from "../queries/useLoadRegistry";

// Usage of generic type parameter T
// https://stackoverflow.com/questions/53203409/how-to-tell-typescript-that-im-returning-an-array-of-arrays-of-the-input-type
const buildTagCollection = <T>(
  array: T[],
  recordExtractor: (unknownFCO: T) => Record<string, string> | undefined // Assumes that tags are always a Record<string, string>
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
    {}
  );

  return tagCollection;
};

const useFeatureViewTagsAggregation = () => {
  const registryUrl = useContext(RegistryPathContext);
  const query = useLoadRegistry(registryUrl);

  const data = useMemo(() => {
    return query.data && query.data.objects && query.data.objects.featureViews
      ? buildTagCollection<FeastFeatureViewType>(
          query.data.objects.featureViews,
          (fv) => {
            return fv.spec.tags;
          }
        )
      : undefined;
  }, [query.data]);

  return {
    ...query,
    data,
  };
};

const useFeatureServiceTagsAggregation = () => {
  const registryUrl = useContext(RegistryPathContext);
  const query = useLoadRegistry(registryUrl);

  const data = useMemo(() => {
    return query.data &&
      query.data.objects &&
      query.data.objects.featureServices
      ? buildTagCollection<FeastFeatureServiceType>(
          query.data.objects.featureServices,
          (fs) => {
            return fs.spec.tags;
          }
        )
      : undefined;
  }, [query.data]);

  return {
    ...query,
    data,
  };
};

export { useFeatureViewTagsAggregation, useFeatureServiceTagsAggregation };
