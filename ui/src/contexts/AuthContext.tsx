import React, {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  useRef,
} from "react";
// @ts-ignore -- keycloak-js types use "exports" field; bundler resolves the JS fine
import Keycloak from "keycloak-js";

interface AuthUser {
  username: string;
  roles: string[];
  groups: string[];
  email?: string;
}

interface AuthContextValue {
  user: AuthUser | null;
  isAuthenticated: boolean;
  isAuthEnabled: boolean;
  isInitializing: boolean;
  logout: () => void;
  keycloak: Keycloak | null;
}

const AuthContext = createContext<AuthContextValue>({
  user: null,
  isAuthenticated: false,
  isAuthEnabled: false,
  isInitializing: true,
  logout: () => {},
  keycloak: null,
});

interface ServerAuthConfig {
  auth_type: string;
  url?: string;
  realm?: string;
  client_id?: string;
  auth_discovery_url?: string;
}

function readServerAuthConfig(): ServerAuthConfig | null {
  // Injected by the Feast UI server (feast ui) into index.html from feature_store.yaml
  const el = document.getElementById("feast-auth-config");
  if (el) {
    try {
      return JSON.parse(el.textContent || "{}");
    } catch {
      /* fall through */
    }
  }

  // Dev mode fallback: fetch from /api/auth-config (served by setupProxy or REST server)
  return null;
}

function extractUser(kc: Keycloak): AuthUser {
  const parsed = kc.tokenParsed as any;
  const clientRoles: string[] =
    parsed?.resource_access?.[kc.clientId!]?.roles || [];
  const realmRoles: string[] = parsed?.realm_access?.roles || [];
  const combined = [...clientRoles, ...realmRoles];

  return {
    username: parsed?.preferred_username || "unknown",
    roles: combined.filter((v, i) => combined.indexOf(v) === i),
    groups: parsed?.groups || [],
    email: parsed?.email,
  };
}

const TOKEN_REFRESH_INTERVAL = 30_000;
const MIN_VALIDITY_SECS = 60;

const AuthProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [kc, setKc] = useState<Keycloak | null>(null);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [isInitializing, setIsInitializing] = useState(true);
  const [isAuthEnabled, setIsAuthEnabled] = useState(false);
  const initStarted = useRef(false);

  const syncUser = useCallback((keycloak: Keycloak) => {
    if (keycloak.authenticated) {
      setUser(extractUser(keycloak));
    }
  }, []);

  const doRefresh = useCallback(
    (keycloak: Keycloak) => {
      keycloak
        .updateToken(MIN_VALIDITY_SECS)
        .then((refreshed: boolean) => {
          if (refreshed) {
            syncUser(keycloak);
          }
        })
        .catch(() => {
          console.warn("Token refresh failed — redirecting to login");
          keycloak.login();
        });
    },
    [syncUser],
  );

  useEffect(() => {
    if (initStarted.current) return;
    initStarted.current = true;

    const initAuth = async () => {
      const config = readServerAuthConfig();

      if (!config || config.auth_type !== "oidc") {
        // Auth is disabled — skip Keycloak, render the app immediately
        setIsAuthEnabled(false);
        setIsInitializing(false);
        return;
      }

      setIsAuthEnabled(true);

      const keycloak = new Keycloak({
        url: config.url || "http://localhost:8080",
        realm: config.realm || "feast",
        clientId: config.client_id || "feast-ui",
      });

      keycloak.onTokenExpired = () => {
        console.info("Access token expired — attempting refresh");
        doRefresh(keycloak);
      };

      keycloak.onAuthRefreshSuccess = () => {
        syncUser(keycloak);
      };

      keycloak.onAuthRefreshError = () => {
        console.warn("Auth refresh error — redirecting to login");
        keycloak.login();
      };

      try {
        const authenticated = await keycloak.init({
          onLoad: "login-required",
          checkLoginIframe: false,
          pkceMethod: "S256",
        });

        setKc(keycloak);
        if (authenticated) {
          syncUser(keycloak);
        }
      } catch (err) {
        console.error("Keycloak init failed:", err);
      }

      setIsInitializing(false);
    };

    initAuth();
  }, [doRefresh, syncUser]);

  // Proactive token refresh
  useEffect(() => {
    if (!kc?.authenticated) return;
    const id = setInterval(() => doRefresh(kc), TOKEN_REFRESH_INTERVAL);
    return () => clearInterval(id);
  }, [kc, doRefresh]);

  // Global fetch interceptor: inject auth header on /api/ calls, handle 401
  useEffect(() => {
    if (!kc) return;

    const originalFetch = window.fetch;
    window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof Request
            ? input.url
            : input.toString();

      const isApiCall = url.includes("/api/");

      if (isApiCall && kc.authenticated && kc.token) {
        const mergedHeaders = new Headers(init?.headers);
        if (!mergedHeaders.has("Authorization")) {
          mergedHeaders.set("Authorization", `Bearer ${kc.token}`);
        }
        init = { ...init, headers: mergedHeaders };
      }

      let response = await originalFetch(input, init);

      if (response.status === 401 && isApiCall) {
        console.warn("API returned 401 — attempting token refresh and retry");
        try {
          await kc.updateToken(5);
          syncUser(kc);
          const retryHeaders = new Headers(init?.headers);
          retryHeaders.set("Authorization", `Bearer ${kc.token}`);
          response = await originalFetch(input, {
            ...init,
            headers: retryHeaders,
          });
        } catch {
          kc.login();
        }
      }

      return response;
    };

    return () => {
      window.fetch = originalFetch;
    };
  }, [kc, syncUser]);

  const logout = useCallback(() => {
    if (kc) {
      kc.logout({ redirectUri: window.location.origin });
    }
  }, [kc]);

  const value: AuthContextValue = {
    user,
    isAuthenticated: !!kc?.authenticated,
    isAuthEnabled,
    isInitializing,
    logout,
    keycloak: kc,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

const useAuth = () => useContext(AuthContext);

export default AuthContext;
export { AuthProvider, useAuth };
export type { AuthUser };
