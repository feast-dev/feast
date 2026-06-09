import React, { useState } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  EuiPageTemplate,
  EuiPopover,
  EuiContextMenu,
  EuiButton,
} from "@elastic/eui";

import { LabelViewIcon } from "../../graphics/LabelViewIcon";
import { useMatchExact, useMatchSubpath } from "../../hooks/useMatchSubpath";
import LabelBrowseTab from "./LabelBrowseTab";
import QualityDashboardTab from "./QualityDashboardTab";
import AnnotateTab from "./AnnotateTab";
import TrainingExportTab from "./TrainingExportTab";
import IntegrationsTab from "./IntegrationsTab";
import BatchUploadTab from "./BatchUploadTab";
import LabelViewLineageTab from "./LabelViewLineageTab";
import FeatureViewVersionsTab from "../feature-views/FeatureViewVersionsTab";
import { useDocumentTitle } from "../../hooks/useDocumentTitle";

const LabelViewInstance = () => {
  const navigate = useNavigate();
  const { labelViewName } = useParams();
  const [isMoreOpen, setIsMoreOpen] = useState(false);

  useDocumentTitle(`${labelViewName} | Label View | Feast`);

  const moreMenuItems = [
    {
      id: "more-panel",
      items: [
        {
          name: "Export",
          icon: "exportAction",
          onClick: () => {
            setIsMoreOpen(false);
            navigate("export");
          },
        },
        {
          name: "Upload",
          icon: "importAction",
          onClick: () => {
            setIsMoreOpen(false);
            navigate("upload");
          },
        },
        {
          name: "Versions",
          icon: "copyClipboard",
          onClick: () => {
            setIsMoreOpen(false);
            navigate("versions");
          },
        },
        {
          name: "Lineage",
          icon: "graphApp",
          onClick: () => {
            setIsMoreOpen(false);
            navigate("lineage");
          },
        },
        {
          name: "Integrations",
          icon: "gear",
          onClick: () => {
            setIsMoreOpen(false);
            navigate("integrations");
          },
        },
      ],
    },
  ];

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Header
        restrictWidth
        iconType={LabelViewIcon}
        pageTitle={labelViewName}
        rightSideItems={[
          <EuiPopover
            key="more-menu"
            button={
              <EuiButton
                size="s"
                iconType="arrowDown"
                iconSide="right"
                onClick={() => setIsMoreOpen(!isMoreOpen)}
              >
                More
              </EuiButton>
            }
            isOpen={isMoreOpen}
            closePopover={() => setIsMoreOpen(false)}
            panelPaddingSize="none"
            anchorPosition="downRight"
          >
            <EuiContextMenu
              initialPanelId="more-panel"
              panels={moreMenuItems}
            />
          </EuiPopover>,
        ]}
        tabs={[
          {
            label: "Labels",
            isSelected: useMatchExact(""),
            onClick: () => {
              navigate("");
            },
          },
          {
            label: "Quality",
            isSelected: useMatchSubpath("quality"),
            onClick: () => {
              navigate("quality");
            },
          },
          {
            label: "Label Data",
            isSelected: useMatchSubpath("annotate"),
            onClick: () => {
              navigate("annotate");
            },
          },
        ]}
      />
      <EuiPageTemplate.Section>
        <Routes>
          <Route path="/" element={<LabelBrowseTab />} />
          <Route path="/quality" element={<QualityDashboardTab />} />
          <Route path="/annotate" element={<AnnotateTab />} />
          <Route path="/export" element={<TrainingExportTab />} />
          <Route path="/upload" element={<BatchUploadTab />} />
          <Route
            path="/versions"
            element={
              <FeatureViewVersionsTab featureViewName={labelViewName || ""} />
            }
          />
          <Route path="/lineage" element={<LabelViewLineageTab />} />
          <Route path="/integrations" element={<IntegrationsTab />} />
        </Routes>
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default LabelViewInstance;
