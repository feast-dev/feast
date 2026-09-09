import React, { useState } from "react";
import {
  EuiModal,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiModalBody,
  EuiTabbedContent,
  EuiTabbedContentTab,
  EuiIcon,
} from "@elastic/eui";
import RegisterDatasetModal from "./RegisterDatasetModal";
import type { RegisterDatasetPayload } from "./RegisterDatasetModal";
import CreateDatasetForm from "./CreateDatasetForm";

interface AddToCatalogModalProps {
  onClose: () => void;
  onLinkSubmit: (data: RegisterDatasetPayload) => Promise<void>;
  isLinkSubmitting: boolean;
  linkError?: string | null;
}

const AddToCatalogModal = ({
  onClose,
  onLinkSubmit,
  isLinkSubmitting,
  linkError,
}: AddToCatalogModalProps) => {
  const [selectedTab, setSelectedTab] = useState<string>("link");

  const tabs: EuiTabbedContentTab[] = [
    {
      id: "link",
      name: "Link Existing",
      prepend: <EuiIcon type="link" size="s" />,
      content: (
        <RegisterDatasetModal
          onClose={onClose}
          onSubmit={onLinkSubmit}
          isSubmitting={isLinkSubmitting}
          error={linkError}
          embedded
        />
      ),
    },
    {
      id: "create",
      name: "Create Dataset",
      prepend: <EuiIcon type="plusInCircle" size="s" />,
      content: <CreateDatasetForm onClose={onClose} />,
    },
  ];

  return (
    <EuiModal onClose={onClose} style={{ width: 780, maxWidth: "90vw" }}>
      <EuiModalHeader>
        <EuiModalHeaderTitle>Add to Catalog</EuiModalHeaderTitle>
      </EuiModalHeader>
      <EuiModalBody>
        <EuiTabbedContent
          tabs={tabs}
          selectedTab={tabs.find((t) => t.id === selectedTab)}
          onTabClick={(tab) => setSelectedTab(tab.id)}
        />
      </EuiModalBody>
    </EuiModal>
  );
};

export default AddToCatalogModal;
