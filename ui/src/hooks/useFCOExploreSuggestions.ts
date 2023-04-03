import { encodeSearchQueryString } from "./encodeSearchQueryString";
import { FEAST_FCO_TYPES } from "../parsers/types";

import { useParams } from "react-router-dom";
import { useFeatureViewTagsAggregation } from "./useTagsAggregation";
import { feast } from "../protos";

interface ExplorationSuggestionItem {
  name: string;
  link: string;
  label: string;
  count: number;
}

interface ExplorationSuggestion {
  title: string;
  items: ExplorationSuggestionItem[];
}

const FCO_TO_URL_NAME_MAP: Record<FEAST_FCO_TYPES, string> = {
  dataSource: "/data-source",
  entity: "/entity",
  featureView: "/feature-view",
  featureService: "/feature-service",
};

const createSearchLink = (
  FCOType: FEAST_FCO_TYPES,
  key: string,
  value: string
) => {
  const URL = FCO_TO_URL_NAME_MAP[FCOType];

  return URL + "?" + encodeSearchQueryString(`${key}:${value}`);
};

const NUMBER_OF_SUGGESTION_GROUPS = 2;
const NUMBER_OF_VALUES_PER_GROUP = 4;

const sortTagByUniqueValues = <T>(
  tagAggregation: Record<string, Record<string, T[]>>
) => {
  return Object.entries(tagAggregation).sort(
    ([a, valuesOfA], [b, valuesOfB]) => {
      return Object.keys(valuesOfB).length - Object.keys(valuesOfA).length;
    }
  );
};

const sortTagsByTotalUsage = <T>(
  tagAggregation: Record<string, Record<string, T[]>>
) => {
  return Object.entries(tagAggregation).sort(
    ([a, valuesOfA], [b, valuesOfB]) => {
      const countOfA = Object.values(valuesOfA).reduce((memo, current) => {
        return memo + current.length;
      }, 0);

      const countOfB = Object.values(valuesOfB).reduce((memo, current) => {
        return memo + current.length;
      }, 0);

      return countOfB - countOfA;
    }
  );
};

const generateExplorationSuggestions = (
  tagAggregation: Record<string, Record<string, feast.core.IFeatureView[]>>,
  projectName: string
) => {
  const suggestions: ExplorationSuggestion[] = [];

  if (tagAggregation) {
    const SortedCandidates =
      sortTagByUniqueValues<feast.core.IFeatureView>(tagAggregation);

    SortedCandidates.slice(0, NUMBER_OF_SUGGESTION_GROUPS).forEach(
      ([selectedTag, selectedTagValuesMap]) => {
        suggestions.push({
          title: `Feature Views by "${selectedTag}"`,
          items: Object.entries(selectedTagValuesMap)
            .sort(([a, entriesOfA], [b, entriesOfB]) => {
              return entriesOfB.length - entriesOfA.length;
            })
            .slice(0, NUMBER_OF_VALUES_PER_GROUP)
            .map(([tagValue, fvEntries]) => {
              return {
                name: tagValue,
                link:
                  `/p/${projectName}` +
                  createSearchLink(
                    FEAST_FCO_TYPES["featureView"],
                    selectedTag,
                    tagValue
                  ),
                label: `Feature Services where ${selectedTag} is '${tagValue}'`,
                count: fvEntries.length,
              };
            }),
        });
      }
    );
  }

  return suggestions;
};

const useFCOExploreSuggestions = () => {
  const query = useFeatureViewTagsAggregation();
  const tagAggregation = query.data;

  const { projectName } = useParams();

  let data: ExplorationSuggestion[] | undefined = undefined;

  if (query.isSuccess && tagAggregation && projectName) {
    data = generateExplorationSuggestions(tagAggregation, projectName);
  }

  return {
    ...query,
    data,
  };
};

export default useFCOExploreSuggestions;
export {
  generateExplorationSuggestions,
  sortTagByUniqueValues,
  sortTagsByTotalUsage,
};
export type { ExplorationSuggestion };
