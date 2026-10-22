import React, { useState } from "react";
import { useParams } from "react-router-dom";
import {
  EuiButtonGroup,
  EuiSpacer,
  EuiPanel,
  EuiText,
  EuiLoadingSpinner,
  EuiCallOut,
  EuiBadge,
  EuiFlexGroup,
  EuiFlexItem,
} from "@elastic/eui";
import ActiveLearningTab from "./ActiveLearningTab";
import RagLabelingMethod from "./RagLabelingMethod";
import ClassificationMethod from "./ClassificationMethod";
import EntityFormMethod from "./EntityFormMethod";
import useAnnotationConfig from "./useAnnotationConfig";

const PROFILE_DESCRIPTIONS: Record<string, string> = {
  "document-span":
    "Load documents, highlight text spans, and label them for RAG retrieval or citation evaluation.",
  "review-edit":
    "Review and edit existing label records in a table view. Supports inline editing and batch push.",
  "entity-form":
    "Fill label fields per entity using a structured form. One record at a time.",
  "active-learning":
    "Surface unlabeled entities from a reference feature view and label them.",
};

const AnnotateTab = () => {
  const { labelViewName } = useParams();
  const {
    data: config,
    isLoading,
    isError,
  } = useAnnotationConfig(labelViewName || "");

  const detectedProfile = config?.profile || "table";

  const availableMethods = React.useMemo(() => {
    const methods: { id: string; label: string }[] = [];

    if (detectedProfile === "document-span") {
      methods.push({ id: "document-span", label: "Document Span" });
      methods.push({ id: "review-edit", label: "Review & Edit" });
      return methods;
    }

    if (detectedProfile === "entity-form") {
      methods.push({ id: "entity-form", label: "Entity Form" });
      methods.push({ id: "review-edit", label: "Review & Edit" });
      methods.push({ id: "active-learning", label: "Active Learning" });
      return methods;
    }

    if (detectedProfile === "active-learning") {
      methods.push({ id: "active-learning", label: "Active Learning" });
      methods.push({ id: "entity-form", label: "Entity Form" });
      methods.push({ id: "review-edit", label: "Review & Edit" });
      return methods;
    }

    methods.push({ id: "review-edit", label: "Review & Edit" });
    methods.push({ id: "active-learning", label: "Active Learning" });
    methods.push({ id: "entity-form", label: "Entity Form" });
    return methods;
  }, [detectedProfile]);

  const [selectedMethod, setSelectedMethod] = useState<string | null>(null);
  const activeMethod =
    selectedMethod || availableMethods[0]?.id || "review-edit";

  if (isLoading) {
    return (
      <EuiFlexGroup alignItems="center" gutterSize="s">
        <EuiFlexItem grow={false}>
          <EuiLoadingSpinner size="m" />
        </EuiFlexItem>
        <EuiFlexItem>
          <EuiText>Loading labeling configuration...</EuiText>
        </EuiFlexItem>
      </EuiFlexGroup>
    );
  }

  if (isError || !config) {
    return (
      <EuiCallOut
        title="Could not load labeling config"
        color="warning"
        iconType="alert"
      >
        <p>
          Falling back to Review &amp; Edit view. Define{" "}
          <code>feast.io/labeling-method</code> in your LabelView tags to
          configure the labeling experience.
        </p>
      </EuiCallOut>
    );
  }

  return (
    <React.Fragment>
      <EuiPanel paddingSize="s" hasBorder={false} color="transparent">
        <EuiFlexGroup alignItems="center" gutterSize="m">
          <EuiFlexItem grow={false}>
            <EuiText size="xs" color="subdued">
              <strong>Labeling Method</strong>
            </EuiText>
          </EuiFlexItem>
          <EuiFlexItem grow={false}>
            <EuiBadge color="hollow">profile: {detectedProfile}</EuiBadge>
          </EuiFlexItem>
        </EuiFlexGroup>
        <EuiSpacer size="s" />
        <EuiButtonGroup
          legend="Select labeling method"
          options={availableMethods}
          idSelected={activeMethod}
          onChange={(id) => setSelectedMethod(id)}
          buttonSize="m"
          isFullWidth={false}
        />
        {PROFILE_DESCRIPTIONS[activeMethod] && (
          <>
            <EuiSpacer size="s" />
            <EuiText size="xs" color="subdued">
              {PROFILE_DESCRIPTIONS[activeMethod]}
            </EuiText>
          </>
        )}
      </EuiPanel>

      <EuiSpacer size="l" />

      {activeMethod === "active-learning" && <ActiveLearningTab />}
      {activeMethod === "document-span" && (
        <RagLabelingMethod annotationConfig={config} />
      )}
      {activeMethod === "review-edit" && <ClassificationMethod />}
      {activeMethod === "entity-form" && (
        <EntityFormMethod annotationConfig={config} />
      )}
    </React.Fragment>
  );
};

export default AnnotateTab;
