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

import { FeatureServiceIcon } from "../../graphics/FeatureServiceIcon";

import FeatureServiceListingTable from "./FeatureServiceListingTable";
import {
  useSearchQuery,
  useTagsWithSuggestions,
  filterInputInterface,
  tagTokenGroupsType,
} from "../../hooks/useSearchInputWithTags";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import FeatureServiceIndexEmptyState from "./FeatureServiceIndexEmptyState";
import TagSearch from "../../components/TagSearch";
import ExportButton from "../../components/ExportButton";
import FeatureServiceFormModal, {
  FeatureServiceFormData,
} from "../../components/FeatureServiceFormModal";
import { useApplyFeatureService } from "../../queries/mutations/useFeatureServiceMutations";
import { useFeatureServiceTagsAggregation } from "../../hooks/useTagsAggregation";
import { feast } from "../../protos";
import useResourceQuery, {
  featureServiceListPath,
  featureViewListPath,
  restFeatureViewsToMergedList,
} from "../../queries/useResourceQuery";

const useLoadFeatureServices = () => {
  const { projectName } = useParams();
  return useResourceQuery<any[]>({
    resourceType: "feature-services-list",
    project: projectName,
    restPath: featureServiceListPath(projectName),
    restSelect: (d) => d.featureServices,
  });
};

const shouldIncludeFSsGivenTokenGroups = (
  entry: feast.core.IFeatureService,
  tagTokenGroups: tagTokenGroupsType,
) => {
  return Object.entries(tagTokenGroups).every(([key, values]) => {
    const entryTagValue = entry?.spec?.tags ? entry.spec.tags[key] : undefined;

    if (entryTagValue) {
      return values.every((value) => {
        return value.length > 0 ? entryTagValue.indexOf(value) >= 0 : true;
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

const formDataToPayload = (
  formData: FeatureServiceFormData,
  project: string,
) => ({
  name: formData.name,
  project,
  features: formData.projections.map((projection) => ({
    feature_view_name: projection.featureViewName,
    feature_names: projection.featureNames,
  })),
  description: formData.description,
  owner: formData.owner,
  tags: Object.fromEntries(
    formData.tags
      .filter((tag) => tag.key.trim())
      .map((tag) => [tag.key, tag.value]),
  ),
});

const Index = () => {
  const { projectName } = useParams();
  const { isLoading, isSuccess, isError, isPermissionDenied, data } =
    useLoadFeatureServices();
  const isAllProjects = projectName === "all";
  const tagAggregationQuery = useFeatureServiceTagsAggregation();

  const featureViewsQuery = useResourceQuery<any[]>({
    resourceType: "feature-views-list-fs-prereq",
    project: projectName,
    restPath: featureViewListPath(projectName),
    restSelect: restFeatureViewsToMergedList,
    enabled: !isAllProjects,
  });

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [prereqWarning, setPrereqWarning] = useState<string | null>(null);
  const applyFeatureService = useApplyFeatureService();

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

  const handleCreateClick = () => {
    const featureViews = featureViewsQuery.data || [];
    if (featureViews.length === 0) {
      setPrereqWarning(
        "Feature services require at least one feature view. Create a feature view first, or proceed and add views later.",
      );
    } else {
      setPrereqWarning(null);
    }
    setIsModalOpen(true);
  };

  const handleCreateSubmit = (formData: FeatureServiceFormData) => {
    const payload = formDataToPayload(formData, projectName || "");
    applyFeatureService.mutate(payload, {
      onSuccess: () => {
        setIsModalOpen(false);
        setErrorMessage(null);
        setPrereqWarning(null);
        setSuccessMessage(
          `Feature service "${formData.name}" created successfully.`,
        );
        setTimeout(() => setSuccessMessage(null), 5000);
      },
      onError: (err: unknown) => {
        const message =
          err instanceof Error ? err.message : "An unexpected error occurred.";
        setErrorMessage(message);
      },
    });
  };

  const showEmptyState = isSuccess && (!data || data.length === 0);

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={FeatureServiceIcon}
        pageTitle="Feature Services"
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
                  Create Feature Service
                </EuiButton>,
              ]),
          <ExportButton
            data={filterResult ?? []}
            fileName="feature_services"
            formats={["json"]}
            key="export"
          />,
        ]}
      />
      <EuiPageTemplate.Section>
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
        {prereqWarning && !isModalOpen && (
          <>
            <EuiCallOut
              title={prereqWarning}
              color="warning"
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
        {isPermissionDenied && (
          <EuiCallOut title="Permission denied" color="warning" iconType="lock">
            <p>You do not have permission to view feature services.</p>
          </EuiCallOut>
        )}
        {isError && !isPermissionDenied && (
          <p>We encountered an error while loading.</p>
        )}
        {showEmptyState && (
          <FeatureServiceIndexEmptyState
            onCreate={isAllProjects ? undefined : handleCreateClick}
          />
        )}
        {isSuccess && filterResult && filterResult.length > 0 && (
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

      {isModalOpen && (
        <FeatureServiceFormModal
          onClose={() => {
            setIsModalOpen(false);
            setErrorMessage(null);
          }}
          onSubmit={handleCreateSubmit}
          isSubmitting={applyFeatureService.isLoading}
          submitError={errorMessage}
        />
      )}
    </EuiPageTemplate>
  );
};

export default Index;
