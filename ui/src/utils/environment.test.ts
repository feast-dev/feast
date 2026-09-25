import { getProcessEnv } from "./environment";

test("returns undefined when process env map is unavailable", () => {
  expect(getProcessEnv("PUBLIC_URL", {}, undefined)).toBeUndefined();
});

test("returns env value when process env contains the key", () => {
  expect(
    getProcessEnv("REACT_APP_FEAST_FEATURE_SERVER_URL", {
      env: {
        REACT_APP_FEAST_FEATURE_SERVER_URL: "http://example:6566",
      },
    }),
  ).toBe("http://example:6566");
});

test("returns value from Vite-prefixed env when available", () => {
  expect(
    getProcessEnv(
      "REACT_APP_FEAST_FEATURE_SERVER_URL",
      { env: {} },
      { VITE_REACT_APP_FEAST_FEATURE_SERVER_URL: "http://vite:6566" },
    ),
  ).toBe("http://vite:6566");
});

test("returns Vite BASE_URL for PUBLIC_URL", () => {
  expect(getProcessEnv("PUBLIC_URL", { env: {} }, { BASE_URL: "/ui/" })).toBe(
    "/ui/",
  );
});

test("returns undefined when env key does not exist", () => {
  expect(getProcessEnv("PUBLIC_URL", { env: {} }, {})).toBeUndefined();
});
