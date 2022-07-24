import { useState } from "react";

import { useQueryParam, StringParam } from "use-query-params";

import { sortTagsByTotalUsage } from "./useFCOExploreSuggestions";

type tagTokenType = Record<"key" | "value", string>;
type tagTokenGroupsType = Record<string, string[]>;
type tagAggregationRecordType =
  | Record<string, Record<string, unknown[]>>
  | undefined;

type SuggestionModes = "KEY" | "VALUE";

interface filterInputInterface {
  tagTokenGroups: tagTokenGroupsType;
  searchTokens: string[];
}

interface TagSuggestionInstance {
  suggestion: string;
  description: string;
}

const generateEmptyStateSuggestions = (
  tagsAggregationData: tagAggregationRecordType
) => {
  if (tagsAggregationData) {
    return sortTagsByTotalUsage(tagsAggregationData).map(
      ([tagKey, recordOfTagValues]) => {
        const uniqueValues = Object.keys(recordOfTagValues).length;
        const totalEntries = Object.values(recordOfTagValues).reduce(
          (memo, current) => {
            return memo + current.length;
          },
          0
        );

        return {
          suggestion: tagKey,
          description: `${uniqueValues} unique tag values. ${totalEntries} total entries.`,
        };
      }
    );
  } else {
    return [];
  }
};

const generateTagKeySuggestions = (
  input: string,
  tagsAggregationData: tagAggregationRecordType
) => {
  if (tagsAggregationData) {
    return Object.entries(tagsAggregationData)
      .filter(([potentialTagKey, summary]) => {
        return potentialTagKey.indexOf(input) >= 0;
      })
      .map(([potentialTagKey, summary]) => {
        const tagValueVariants = Object.entries(summary);

        return {
          suggestion: potentialTagKey,
          description: `${tagValueVariants.length} different tag values`,
        };
      });
  } else {
    return [];
  }
};

const generateTagValueSuggestions = (
  input: string,
  tagsAggregationData: tagAggregationRecordType
) => {
  if (tagsAggregationData) {
    const [currentTagKey, currentTagValue] = input.split(":");
    const entriesWithTagKey = tagsAggregationData[currentTagKey];

    const summarizeCallback = (entry: unknown[]) => {
      const potentialTagKey = entry[0] as string;
      const summary = entry[1] as unknown[];

      return {
        suggestion: potentialTagKey,
        description: `${summary.length} entries`,
      };
    };

    if (entriesWithTagKey) {
      if (currentTagValue && currentTagValue.length > 0) {
        return Object.entries(entriesWithTagKey)
          .filter(([potentialTagValue, entries]) => {
            return (
              potentialTagValue.indexOf(currentTagValue) >= 0 // &&
              // potentialTagValue !== currentTagValue // Don't show exact matches, b/c that means we probably already selected it
            );
          })
          .map(summarizeCallback);
      } else {
        return Object.entries(entriesWithTagKey).map(summarizeCallback);
      }
    } else {
      return [];
    }
  } else {
    return [];
  }
};

function getAllSpacePositions(s: string) {
  const indices: number[] = [];
  while (
    s.indexOf(" ", indices.length ? indices[indices.length - 1] + 1 : 0) !==
      -1 &&
    indices.length < 100
  ) {
    const position = indices[indices.length - 1] || 0;
    const index = s.indexOf(" ", position + 1);
    indices.push(index);
  }
  return indices;
}

interface TagSplitterReturnInterface {
  chunks: string[];
  tokenInFocusIndex: number;
  currentTag: string;
}

const parseTokenInput = (
  cursorPosition: number | undefined,
  tagsString: string
): TagSplitterReturnInterface => {
  // Get where the spaces in the tagString, plus a start and end value
  // e.g. "A:a B:b" would return
  // [0, 3, 7]
  const chunks = tagsString.split(" ");

  const allSpacePositions = [0]
    .concat(getAllSpacePositions(tagsString))
    .concat(tagsString.length + 1);

  let tokenInFocusIndex = 0;
  if (cursorPosition) {
    tokenInFocusIndex = allSpacePositions.findIndex((value, index) => {
      return (
        cursorPosition >= value &&
        cursorPosition <= allSpacePositions[index + 1]
      );
    });
  }

  const currentTag = chunks[tokenInFocusIndex] || "";

  return {
    currentTag,
    chunks,
    tokenInFocusIndex,
  };
};

const useSearchQuery = () => {
  const [query, setQuery] = useQueryParam("q", StringParam);
  const searchString = query || "";

  const searchTokens = searchString.split(" ").filter((t) => t.length >= 3);

  const setSearchString = (d: string) => {
    setQuery(d);
  };

  return {
    searchString,
    searchTokens,
    setSearchString,
  };
};

const useTagsWithSuggestions = (
  tagsAggregationData: tagAggregationRecordType
) => {
  const [rawtagsString, setTagsStringParam] = useQueryParam(
    "tags",
    StringParam
  );

  const tagsString = rawtagsString || "";

  // Spaces in the beginning of the string
  // really messes with parseTokenInput(). Just prevent it.
  const setTagsString = (s: string) => {
    setTagsStringParam(s.trimStart());
  };

  const [cursorPosition, setCursor] = useState<number | undefined>(undefined);
  const setCursorPosition = (position: number | undefined) => {
    setCursor(position);
  };

  // Parse input into tokens, and detect which token
  // we are focused on given the current cursor position
  const { chunks, tokenInFocusIndex, currentTag } = parseTokenInput(
    cursorPosition,
    tagsString
  );

  const suggestionMode: SuggestionModes =
    currentTag.indexOf(":") < 0 ? "KEY" : "VALUE";

  let tagSuggestions: TagSuggestionInstance[] = [];
  if (tagsAggregationData) {
    if (currentTag.length > 0) {
      if (suggestionMode === "KEY") {
        tagSuggestions = generateTagKeySuggestions(
          currentTag,
          tagsAggregationData
        );
      } else {
        tagSuggestions = generateTagValueSuggestions(
          currentTag,
          tagsAggregationData
        );
      }
    } else {
      // Current Tag is empty
      tagSuggestions = generateEmptyStateSuggestions(tagsAggregationData);
    }
  }

  // Helper method for accepting suggestions
  const setSuggestionAtPositionInTagsString = (
    suggestion: string,
    position: number
  ) => {
    const nextTagsTokens = chunks.slice(0);
    nextTagsTokens[position] = suggestion;

    setTagsString(nextTagsTokens.join(" "));
  };

  const acceptSuggestion = (suggestion: TagSuggestionInstance) => {
    if (suggestionMode === "KEY") {
      const newKeyText = suggestion.suggestion + ":";

      setSuggestionAtPositionInTagsString(newKeyText, tokenInFocusIndex);
    } else {
      const [currentTagKey] = currentTag.split(":");

      const newTagText = `${currentTagKey}:` + suggestion.suggestion;

      setSuggestionAtPositionInTagsString(newTagText, tokenInFocusIndex);
    }
  };

  const tagKeysSet = new Set<string>();

  const tagTokens: tagTokenType[] = chunks
    .filter((chunk: string) => {
      return chunk.indexOf(":") > 0;
    })
    .map((chunk) => {
      const parts = chunk.split(":");
      tagKeysSet.add(parts[0]);

      return {
        key: parts[0],
        value: parts[1],
      };
    });

  const tagTokenGroups = tagTokens.reduce(
    (memo: Record<string, string[]>, current) => {
      if (memo[current.key]) {
        memo[current.key].push(current.value);
      } else {
        memo[current.key] = [current.value];
      }

      return memo;
    },
    {}
  );

  return {
    setCursorPosition,
    currentTag,
    tagsString,
    setTagsString,
    tagTokens,
    tagTokenGroups,
    tagKeysSet, // Used to determine which columns to add to search results,
    suggestionMode,
    tagSuggestions,
    acceptSuggestion,
  };
};

export { useTagsWithSuggestions, useSearchQuery };
export type {
  filterInputInterface,
  tagTokenGroupsType,
  TagSuggestionInstance,
  SuggestionModes,
};
