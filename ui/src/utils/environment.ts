type ProcessLike = {
  env?: Record<string, string | undefined>;
};

type ViteEnvLike = Record<string, string | boolean | undefined>;

const getDefaultProcess = (): ProcessLike | undefined => {
  if (typeof process === "undefined") {
    return undefined;
  }
  return process;
};

const getDefaultViteEnv = (): ViteEnvLike | undefined => {
  if (typeof import.meta === "undefined" || !import.meta.env) {
    return undefined;
  }

  return import.meta.env as ViteEnvLike;
};

const getStringEnvValue = (
  envValue: string | boolean | undefined,
): string | undefined => {
  if (typeof envValue !== "string") {
    return undefined;
  }

  return envValue;
};

const getViteEnvValue = (
  envVarName: string,
  viteEnv: ViteEnvLike | undefined,
): string | undefined => {
  if (!viteEnv) {
    return undefined;
  }

  if (envVarName === "PUBLIC_URL") {
    return (
      getStringEnvValue(viteEnv.BASE_URL) ??
      getStringEnvValue(viteEnv.VITE_PUBLIC_URL)
    );
  }

  return (
    getStringEnvValue(viteEnv[`VITE_${envVarName}`]) ??
    getStringEnvValue(viteEnv[envVarName])
  );
};

export const getProcessEnv = (
  envVarName: string,
  processLike: ProcessLike | undefined = getDefaultProcess(),
  viteEnv: ViteEnvLike | undefined = getDefaultViteEnv(),
): string | undefined => {
  const viteEnvValue = getViteEnvValue(envVarName, viteEnv);
  if (viteEnvValue !== undefined) {
    return viteEnvValue;
  }

  if (!processLike?.env) {
    return undefined;
  }

  return getStringEnvValue(processLike.env[envVarName]);
};
