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

const restPost = async <T>(
  baseUrl: string,
  path: string,
  body: unknown,
  fetchOptions?: FetchOptions,
): Promise<T> => {
  const url = `${baseUrl}${path}`;
  const headers: Record<string, string> = {
    Accept: "application/json",
    "Content-Type": "application/json",
    ...fetchOptions?.headers,
  };

  const res = await fetch(url, {
    method: "POST",
    headers,
    credentials: fetchOptions?.credentials,
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new RestApiError(`REST API error: ${res.status} ${text}`, res.status);
  }

  return res.json();
};

const restDelete = async <T>(
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
    method: "DELETE",
    headers,
    credentials: fetchOptions?.credentials,
  });

  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new RestApiError(`REST API error: ${res.status} ${text}`, res.status);
  }

  return res.json();
};

export default restFetch;
export { RestApiError, restPost, restDelete };
