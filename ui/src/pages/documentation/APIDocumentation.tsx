import React, { useEffect, useState } from "react";
import ReactMarkdown from "react-markdown";
import { EuiPanel, EuiLoadingSpinner, EuiText } from "@elastic/eui";
import DocumentationService from "../../services/DocumentationService";

const APIDocumentation = () => {
  const [content, setContent] = useState<string>("");
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadDocumentation = async () => {
      try {
        setIsLoading(true);
        const markdown = await DocumentationService.fetchAPIDocumentation();
        setContent(markdown);
        setError(null);
      } catch (err) {
        setError("Failed to load API documentation");
        console.error(err);
      } finally {
        setIsLoading(false);
      }
    };

    loadDocumentation();
  }, []);

  if (isLoading) {
    return (
      <EuiPanel paddingSize="l">
        <EuiLoadingSpinner size="xl" />
      </EuiPanel>
    );
  }

  if (error) {
    return (
      <EuiPanel color="danger" paddingSize="l">
        <EuiText>
          <p>{error}</p>
        </EuiText>
      </EuiPanel>
    );
  }

  return (
    <EuiPanel paddingSize="l">
      <EuiText className="documentation-content">
        <ReactMarkdown>{content}</ReactMarkdown>
      </EuiText>
    </EuiPanel>
  );
};

export default APIDocumentation;
