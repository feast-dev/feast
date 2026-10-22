import React from "react";

interface FeatureFlags {
  enabledFeatureStatistics?: boolean;
}

const FeatureFlagsContext = React.createContext<FeatureFlags>({});

export default FeatureFlagsContext;
export type { FeatureFlags };
