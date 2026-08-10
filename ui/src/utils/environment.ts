type ProcessLike = {
  env?: Record<string, string | undefined>;
};

const getDefaultProcess = (): ProcessLike | undefined => {
  if (typeof process === "undefined") {
    return undefined;
  }
  return process;
};

export const getProcessEnv = (
  envVarName: string,
  processLike: ProcessLike | undefined = getDefaultProcess(),
): string | undefined => {
  if (!processLike?.env) {
    return undefined;
  }

  const envValue = processLike.env[envVarName];
  if (typeof envValue !== "string") {
    return undefined;
  }

  return envValue;
};
