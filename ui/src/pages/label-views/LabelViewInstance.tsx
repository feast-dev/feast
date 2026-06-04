import React from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import { EuiBadge, EuiPageTemplate } from "@elastic/eui";

import { LabelViewIcon } from "../../graphics/LabelViewIcon";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import LabelViewOverviewTab from "./LabelViewOverviewTab";
import LabelBrowseTab from "./LabelBrowseTab";
import QualityDashboardTab from "./QualityDashboardTab";
import ActiveLearningTab from "./ActiveLearningTab";
import TrainingExportTab from "./TrainingExportTab";
import IntegrationsTab from "./IntegrationsTab";
import BatchUploadTab from "./BatchUploadTab";
import LabelViewLineageTab from "./LabelViewLineageTab";
import FeatureViewVersionsTab from "../feature-views/FeatureViewVersionsTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";
import useLoadLabelView from "./useLoadLabelView";

const LabelViewInstance = () => {
  const navigate = useNavigate();
  const { labelViewName } = useParams();
  const { data } = useLoadLabelView(labelViewName || "");

  useDocumentTitle(`${labelViewName} | Label View | Feast`);

  const versionNumber = data?.meta?.currentVersionNumber;

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={LabelViewIcon}
        pageTitle={
          <>
            Label View: {labelViewName}
            {versionNumber != null && versionNumber > 0 && (
              <EuiBadge color="hollow" style={{ marginLeft: 8 }}>
                v{versionNumber}
              </EuiBadge>
            )}
          </>
        }
        tabs={[
          {
            label: "Overview",
            isSelected: useMatchExact(""),
            onClick: () => {
              navigate("");
            },
          },
          {
            label: "Lineage",
            isSelected: useMatchSubpath("lineage"),
            onClick: () => {
              navigate("lineage");
            },
          },
          {
            label: "Versions",
            isSelected: useMatchSubpath("versions"),
            onClick: () => {
              navigate("versions");
            },
          },
          {
            label: "Browse",
            isSelected: useMatchExact("browse"),
            onClick: () => {
              navigate("browse");
            },
          },
          {
            label: "Quality",
            isSelected: useMatchExact("quality"),
            onClick: () => {
              navigate("quality");
            },
          },
          {
            label: "Active Learning",
            isSelected: useMatchExact("active-learning"),
            onClick: () => {
              navigate("active-learning");
            },
          },
          {
            label: "Training Export",
            isSelected: useMatchExact("export"),
            onClick: () => {
              navigate("export");
            },
          },
          {
            label: "Batch Upload",
            isSelected: useMatchExact("upload"),
            onClick: () => {
              navigate("upload");
            },
          },
          {
            label: "Integrations",
            isSelected: useMatchExact("integrations"),
            onClick: () => {
              navigate("integrations");
            },
          },
        ]}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route path="/" element={<LabelViewOverviewTab />} />
          <Route path="/lineage" element={<LabelViewLineageTab />} />
          <Route
            path="/versions"
            element={
              <FeatureViewVersionsTab featureViewName={labelViewName || ""} />
            }
          />
          <Route path="/browse" element={<LabelBrowseTab />} />
          <Route path="/quality" element={<QualityDashboardTab />} />
          <Route path="/active-learning" element={<ActiveLearningTab />} />
          <Route path="/export" element={<TrainingExportTab />} />
          <Route path="/upload" element={<BatchUploadTab />} />
          <Route path="/integrations" element={<IntegrationsTab />} />
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default LabelViewInstance;
