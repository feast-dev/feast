import React, { useState } from "react";
import { useParams } from "react-router-dom";

import {
  EuiPageTemplate,
  EuiLoadingSpinner,
  EuiSpacer,
  EuiTitle,
  EuiFieldSearch,
  EuiFlexGroup,
  EuiFlexItem,
  EuiButton,
  EuiCallOut,
} from "@elastic/eui";

import { FeatureViewIcon } from "../../graphics/FeatureViewIcon";

import FeatureViewListingTable from "./FeatureViewListingTable";
import {
  filterInputInterface,
  useSearchQuery,
  useTagsWithSuggestions,
} from "../../hooks/useSearchInputWithTags";
import { genericFVType, regularFVInterface } from "../../parsers/mergedFVTypes";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import FeatureViewIndexEmptyState from "./FeatureViewIndexEmptyState";
import { useFeatureViewTagsAggregation } from "../../hooks/useTagsAggregation";
import TagSearch from "../../components/TagSearch";
import ExportButton from "../../components/ExportButton";
import FeatureViewFormModal, {
  FeatureViewFormData,
} from "../../components/FeatureViewFormModal";
import { useApplyFeatureView } from "../../queries/mutations/useFeatureViewMutations";
import useResourceQuery, {
  featureViewListPath,
  restFeatureViewsToMergedList,
  entityListPath,
  dataSourceListPath,
} from "../../queries/useResourceQuery";

const useLoadFeatureViews = () => {
  const { projectName } = useParams();
  return useResourceQuery<genericFVType[]>({
    resourceType: "feature-views-list",
    project: projectName,
    restPath: featureViewListPath(projectName),
    restSelect: restFeatureViewsToMergedList,
  });
};

const shouldIncludeFVsGivenTokenGroups = (
  entry: regularFVInterface,
  tagTokenGroups: Record<string, string[]>,
) => {
  return Object.entries(tagTokenGroups).every(([key, values]) => {
    const tags = entry?.object?.spec?.tags;
    const entryTagValue = tags ? (tags as any)[key] : undefined;

    if (entryTagValue) {
      return values.every((value) => {
        return value.length > 0 ? entryTagValue.indexOf(value) >= 0 : true;
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
        return false;
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

const TTL_UNITS: Record<string, number> = {
  days: 86400,
  hours: 3600,
  minutes: 60,
  seconds: 1,
};

const formDataToPayload = (formData: FeatureViewFormData, project: string) => ({
  name: formData.name,
  project,
  entities: formData.entities,
  features: formData.features.map((f) => ({
    name: f.name,
    value_type: parseInt(f.valueType, 10),
    description: f.description,
  })),
  batch_source: formData.batchSource,
  ttl_seconds: formData.ttlValue * (TTL_UNITS[formData.ttlUnit] || 1),
  online: formData.online,
  description: formData.description,
  owner: formData.owner,
  tags: Object.fromEntries(
    formData.tags.filter((t) => t.key.trim()).map((t) => [t.key, t.value]),
  ),
});

const Index = () => {
  const { projectName } = useParams();
  const { isLoading, isSuccess, isError, data } = useLoadFeatureViews();
  const isAllProjects = projectName === "all";

  const entitiesQuery = useResourceQuery<any[]>({
    resourceType: "entities-list-fv-prereq",
    project: projectName,
    restPath: entityListPath(projectName),
    restSelect: (d) => d.entities,
  });
  const dataSourcesQuery = useResourceQuery<any[]>({
    resourceType: "data-sources-list-fv-prereq",
    project: projectName,
    restPath: dataSourceListPath(projectName),
    restSelect: (d) => d.dataSources,
  });

  const tagAggregationQuery = useFeatureViewTagsAggregation();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [prereqWarning, setPrereqWarning] = useState<string | null>(null);
  const applyFeatureView = useApplyFeatureView();

  const handleCreateClick = () => {
    const missingDeps: string[] = [];
    const entities = entitiesQuery.data || [];
    const dataSources = dataSourcesQuery.data || [];

    if (entities.length === 0) missingDeps.push("entities");
    if (dataSources.length === 0) missingDeps.push("data sources");

    if (missingDeps.length > 0) {
      setPrereqWarning(
        `Feature views require at least one entity and one data source. Missing: ${missingDeps.join(" and ")}. You can still proceed — the form will let you create them inline.`,
      );
    }
    setIsModalOpen(true);
  };

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

  const handleCreateSubmit = (formData: FeatureViewFormData) => {
    const payload = formDataToPayload(formData, projectName || "");
    applyFeatureView.mutate(payload, {
      onSuccess: () => {
        setIsModalOpen(false);
        setErrorMessage(null);
        setPrereqWarning(null);
        setSuccessMessage(
          `Feature view "${formData.name}" created successfully.`,
        );
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setErrorMessage(message);
        setTimeout(() => setErrorMessage(null), 8000);
      },
    });
  };

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureViewIcon}
        pageTitle="Feature Views"
        rightSideItems={[
          ...(isAllProjects
            ? []
            : [
                <EuiButton
                  fill
                  iconType="plus"
                  onClick={handleCreateClick}
                  key="create"
                >
                  Create Feature View
                </EuiButton>,
              ]),
          <ExportButton
            data={filterResult ?? []}
            fileName="feature_views"
            formats={["json"]}
            key="export"
          />,
        ]}
      />
      <EuiPageTemplate.Section>
        {prereqWarning && (
          <>
            <EuiCallOut
              title="Missing prerequisites"
              color="warning"
              iconType="alert"
              size="s"
            >
              <p>{prereqWarning}</p>
            </EuiCallOut>
            <EuiSpacer size="m" />
          </>
        )}
        {successMessage && (
          <>
            <EuiCallOut
              title={successMessage}
              color="success"
              iconType="check"
              size="s"
            />
            <EuiSpacer size="m" />
          </>
        )}
        {errorMessage && (
          <>
            <EuiCallOut
              title={errorMessage}
              color="danger"
              iconType="alert"
              size="s"
            />
            <EuiSpacer size="m" />
          </>
        )}
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

      {isModalOpen && (
        <FeatureViewFormModal
          onClose={() => {
            setIsModalOpen(false);
            setPrereqWarning(null);
          }}
          onSubmit={handleCreateSubmit}
        />
      )}
    </EuiPageTemplate>
  );
};

export default Index;
