import { useResolvedPath, useMatch } from "react-router-dom";

const useMatchSubpath = (to: string) => {
  const resolved = useResolvedPath(to);

  return useMatch({ path: resolved.pathname, end: false }) !== null;
};

const useMatchExact = (to: string) => {
  const resolved = useResolvedPath(to);

  return useMatch({ path: resolved.pathname, end: true }) !== null;
};

export { useMatchSubpath, useMatchExact };
