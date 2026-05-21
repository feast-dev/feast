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
}

const FormModal: React.FC<FormModalProps> = ({
  title,
  submitLabel,
  onClose,
  onSubmit,
  children,
  width = 600,
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
        <EuiButtonEmpty onClick={onClose}>Cancel</EuiButtonEmpty>
        <EuiButton fill onClick={onSubmit}>
          {submitLabel}
        </EuiButton>
      </EuiModalFooter>
    </EuiModal>
  );
};

export default FormModal;
