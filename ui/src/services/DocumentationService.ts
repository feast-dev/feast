const DocumentationService = {
  async fetchCLIDocumentation(): Promise<string> {
    try {
      const response = await fetch(`/docs/reference/feast-cli-commands.md`);
      if (!response.ok) {
        throw new Error(`Failed to fetch CLI documentation: ${response.statusText}`);
      }
      return await response.text();
    } catch (error) {
      console.error("Error fetching CLI documentation:", error);
      return "# Error\nFailed to load CLI documentation.";
    }
  },
  
  async fetchSDKDocumentation(): Promise<string> {
    try {
      const response = await fetch(`/docs/reference/feature-repository/README.md`);
      if (!response.ok) {
        throw new Error(`Failed to fetch SDK documentation: ${response.statusText}`);
      }
      return await response.text();
    } catch (error) {
      console.error("Error fetching SDK documentation:", error);
      return "# Error\nFailed to load SDK documentation.";
    }
  },
  
  async fetchAPIDocumentation(): Promise<string> {
    try {
      const response = await fetch(`/docs/reference/feature-servers/README.md`);
      if (!response.ok) {
        throw new Error(`Failed to fetch API documentation: ${response.statusText}`);
      }
      return await response.text();
    } catch (error) {
      console.error("Error fetching API documentation:", error);
      return "# Error\nFailed to load API documentation.";
    }
  },
};

export default DocumentationService;
