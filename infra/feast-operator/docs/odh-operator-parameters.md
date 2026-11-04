# Open Data Hub / RHOAI operator parameters

These values are supplied through the Feast operator **`params.env`** files in the **ODH** and **RHOAI** overlays (`config/overlays/odh/params.env`, `config/overlays/rhoai/params.env`). The Open Data Hub operator updates keys in `params.env` before rendering; Kustomize **`replacements`** copy them into the controller Deployment.

## `OIDC_ISSUER_URL`

**Purpose:** OIDC issuer URL when the OpenShift cluster uses external OIDC (for example Keycloak). The Feast operator process receives it as the **`OIDC_ISSUER_URL`** environment variable. An empty value means the cluster is not using external OIDC in this integration path (OpenShift OAuth / default behavior).

**Manifest parameter:** `OIDC_ISSUER_URL` in `params.env`.

**Injected into:** `controller-manager` Deployment, `manager` container.

**Set by:** Open Data Hub operator (Feast component reconcile), from `GatewayConfig.spec.oidc.issuerURL` when cluster authentication is OIDC.

**Consumption:** Operator code should read `os.Getenv("OIDC_ISSUER_URL")` (or equivalent) where JWKS / OIDC discovery is required for managed workloads.
