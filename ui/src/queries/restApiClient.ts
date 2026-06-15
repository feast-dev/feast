import type { FetchOptions } from "../contexts/DataModeContext";

class RestApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.name = "RestApiError";
    this.status = status;
  }
}

const restFetch = async <T>(
  baseUrl: string,
  path: string,
  fetchOptions?: FetchOptions,
): Promise<T> => {
  const url = `${baseUrl}${path}`;
  const headers: Record<string, string> = {
    Accept: "application/json",
    ...fetchOptions?.headers,
  };

  const res = await fetch(url, {
    method: "GET",
    headers,
    credentials: fetchOptions?.credentials,
  });

  if (!res.ok) {
    throw new RestApiError(
      `REST API error: ${res.status} ${res.statusText}`,
      res.status,
    );
  }

  return res.json();
};

export default restFetch;
export { RestApiError };
