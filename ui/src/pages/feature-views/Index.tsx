import React, { useContext } from "react";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiSpacer,
  EuiTitle,
  EuiFieldSearch,
  EuiFlexGroup,
  EuiFlexItem,
} from "@elastic/eui";

import { FeatureViewIcon } from "../../graphics/FeatureViewIcon";

import useLoadRegistry from "../../queries/useLoadRegistry";
import FeatureViewListingTable from "./FeatureViewListingTable";
import {
  filterInputInterface,
  useSearchQuery,
  useTagsWithSuggestions,
} from "../../hooks/useSearchInputWithTags";
import { genericFVType, regularFVInterface } from "../../parsers/mergedFVTypes";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import FeatureViewIndexEmptyState from "./FeatureViewIndexEmptyState";
import { useFeatureViewTagsAggregation } from "../../hooks/useTagsAggregation";
import TagSearch from "../../components/TagSearch";

const useLoadFeatureViews = () => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.mergedFVList;

  return {
    ...registryQuery,
    data,
  };
};

const shouldIncludeFVsGivenTokenGroups = (
  entry: regularFVInterface,
  tagTokenGroups: Record<string, string[]>,
) => {
  return Object.entries(tagTokenGroups).every(([key, values]) => {
    const entryTagValue = entry?.object?.spec!.tags
      ? entry.object.spec.tags[key]
      : undefined;

    if (entryTagValue) {
      return values.every((value) => {
        return value.length > 0 ? entryTagValue.indexOf(value) >= 0 : true; // Don't filter if the string is empty
      });
    } else {
      return false;
    }
  });
};

const filterFn = (data: genericFVType[], filterInput: filterInputInterface) => {
  let filteredByTags = data;

  if (Object.keys(filterInput.tagTokenGroups).length) {
    filteredByTags = data.filter((entry) => {
      if (entry.type === "regular") {
        return shouldIncludeFVsGivenTokenGroups(
          entry,
          filterInput.tagTokenGroups,
        );
      } else {
        return false; // ODFVs don't have tags yet
      }
    });
  }

  if (filterInput.searchTokens.length) {
    return filteredByTags.filter((entry) => {
      return filterInput.searchTokens.find((token) => {
        return token.length >= 3 && entry.name.indexOf(token) >= 0;
      });
    });
  }

  return filteredByTags;
};

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadFeatureViews();
  const tagAggregationQuery = useFeatureViewTagsAggregation();

  useDocumentTitle(`Feature Views | Feast`);

  const { searchString, searchTokens, setSearchString } = useSearchQuery();

  const {
    currentTag,
    tagsString,
    tagTokenGroups,
    tagKeysSet,
    tagSuggestions,
    suggestionMode,
    setTagsString,
    acceptSuggestion,
    setCursorPosition,
  } = useTagsWithSuggestions(tagAggregationQuery.data);

  const filterResult = data
    ? filterFn(data, { tagTokenGroups, searchTokens })
    : data;

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureViewIcon}
        pageTitle="Feature Views"
      />
      <EuiPageTemplate.Section>
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && data?.length === 0 && <FeatureViewIndexEmptyState />}
        {isSuccess && data && data.length > 0 && filterResult && (
          <React.Fragment>
            <EuiFlexGroup>
              <EuiFlexItem grow={2}>
                <EuiTitle size="xs">
                  <h2>Search</h2>
                </EuiTitle>
                <EuiFieldSearch
                  value={searchString}
                  fullWidth={true}
                  onChange={(e) => {
                    setSearchString(e.target.value);
                  }}
                />
              </EuiFlexItem>
              <EuiFlexItem grow={3}>
                <TagSearch
                  currentTag={currentTag}
                  tagsString={tagsString}
                  setTagsString={setTagsString}
                  acceptSuggestion={acceptSuggestion}
                  tagSuggestions={tagSuggestions}
                  suggestionMode={suggestionMode}
                  setCursorPosition={setCursorPosition}
                />
              </EuiFlexItem>
            </EuiFlexGroup>
            <EuiSpacer size="m" />
            <FeatureViewListingTable
              tagKeysSet={tagKeysSet}
              featureViews={filterResult}
            />
          </React.Fragment>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default Index;
