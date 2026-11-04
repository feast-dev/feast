import React from "react";
import {
  EuiModal,
  EuiModalHeader,
  EuiModalHeaderTitle,
  EuiModalBody,
  EuiModalFooter,
  EuiButton,
  EuiButtonEmpty,
  EuiForm,
} from "@elastic/eui";

interface FormModalProps {
  title: string;
  submitLabel: string;
  onClose: () => void;
  onSubmit: () => void;
  children: React.ReactNode;
  width?: number;
  isSubmitting?: boolean;
}

const FormModal: React.FC<FormModalProps> = ({
  title,
  submitLabel,
  onClose,
  onSubmit,
  children,
  width = 600,
  isSubmitting = false,
}) => {
  return (
    <EuiModal onClose={onClose} style={{ width }}>
      <EuiModalHeader>
        <EuiModalHeaderTitle>{title}</EuiModalHeaderTitle>
      </EuiModalHeader>

      <EuiModalBody>
        <EuiForm component="form">{children}</EuiForm>
      </EuiModalBody>

      <EuiModalFooter>
        <EuiButtonEmpty onClick={onClose} disabled={isSubmitting}>
          Cancel
        </EuiButtonEmpty>
        <EuiButton fill onClick={onSubmit} isLoading={isSubmitting}>
          {submitLabel}
        </EuiButton>
      </EuiModalFooter>
    </EuiModal>
  );
};

export default FormModal;
