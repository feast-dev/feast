import React, { useContext } from "react";
import { useParams } from "react-router-dom";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiTitle,
  EuiSpacer,
  EuiFlexGroup,
  EuiFlexItem,
  EuiFieldSearch,
  EuiButton,
} from "@elastic/eui";

import { FeatureServiceIcon } from "../../graphics/FeatureServiceIcon";

import useLoadRegistry from "../../queries/useLoadRegistry";
import FeatureServiceListingTable from "./FeatureServiceListingTable";
import {
  useSearchQuery,
  useTagsWithSuggestions,
  filterInputInterface,
  tagTokenGroupsType,
} from "../../hooks/useSearchInputWithTags";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import RegistryPathContext from "../../contexts/RegistryPathContext";
import FeatureServiceIndexEmptyState from "./FeatureServiceIndexEmptyState";
import TagSearch from "../../components/TagSearch";
import ExportButton from "../../components/ExportButton";
import { useFeatureServiceTagsAggregation } from "../../hooks/useTagsAggregation";
import { feast } from "../../protos";

const useLoadFeatureServices = () => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);

  const data =
    registryQuery.data === undefined
      ? undefined
      : registryQuery.data.objects.featureServices;

  return {
    ...registryQuery,
    data,
  };
};

const shouldIncludeFSsGivenTokenGroups = (
  entry: feast.core.IFeatureService,
  tagTokenGroups: tagTokenGroupsType,
) => {
  return Object.entries(tagTokenGroups).every(([key, values]) => {
    const entryTagValue = entry?.spec?.tags ? entry.spec.tags[key] : undefined;

    if (entryTagValue) {
      return values.every((value) => {
        return value.length > 0 ? entryTagValue.indexOf(value) >= 0 : true; // Don't filter if the string is empty
      });
    } else {
      return false;
    }
  });
};

const filterFn = (
  data: feast.core.IFeatureService[],
  filterInput: filterInputInterface,
) => {
  let filteredByTags = data;

  if (Object.keys(filterInput.tagTokenGroups).length) {
    filteredByTags = data.filter((entry) => {
      return shouldIncludeFSsGivenTokenGroups(
        entry,
        filterInput.tagTokenGroups,
      );
    });
  }

  if (filterInput.searchTokens.length) {
    return filteredByTags.filter((entry) => {
      return filterInput.searchTokens.find((token) => {
        return token.length >= 3 && entry?.spec?.name?.indexOf(token)! >= 0;
      });
    });
  }

  return filteredByTags;
};

const Index = () => {
  const { isLoading, isSuccess, isError, data } = useLoadFeatureServices();
  const tagAggregationQuery = useFeatureServiceTagsAggregation();

  useDocumentTitle(`Feature Services | Feast`);

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
        iconType={FeatureServiceIcon}
        pageTitle="Feature Services"
        rightSideItems={[
          <ExportButton
            data={filterResult ?? []}
            fileName="feature_services"
            formats={["json"]}
          />,
          <EuiButton
            iconType="plusInCircle"
            onClick={() => {
              const { projectName } = useParams();
              window.location.href = `/p/${projectName}/feature-service/create`;
            }}
          >
            Create Feature Service
          </EuiButton>,
        ]}
      />
      <EuiPageTemplate.Section>
        {isLoading && (
          <p>
            <EuiLoadingSpinner size="m" /> Loading
          </p>
        )}
        {isError && <p>We encountered an error while loading.</p>}
        {isSuccess && !data && <FeatureServiceIndexEmptyState />}
        {isSuccess && filterResult && (
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
            <FeatureServiceListingTable
              featureServices={filterResult}
              tagKeysSet={tagKeysSet}
            />
          </React.Fragment>
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default Index;
