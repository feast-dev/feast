import logging
import os
import re
from typing import Optional

import requests as http_requests
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_CA_CERT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt"
_CLUSTER_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
_DEFAULT_THANOS_URL = "https://thanos-querier.openshift-monitoring.svc:9091"
_METRICS_PORT = 8000


def _read_sa_token() -> Optional[str]:
    try:
        with open(_SA_TOKEN_PATH) as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def _get_ca_bundle() -> str:
    for path in (_CA_CERT_PATH, _CLUSTER_CA_PATH):
        if os.path.exists(path):
            return path
    return ""


def _parse_prometheus_text(text: str) -> dict:
    """Parse Prometheus exposition format into structured metric families."""
    metrics: dict = {}
    current_type = ""

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("# HELP"):
            parts = line.split(None, 3)
        elif line.startswith("# TYPE"):
            parts = line.split(None, 3)
            current_type = parts[3] if len(parts) > 3 else "untyped"
        elif not line.startswith("#"):
            match = re.match(
                r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([^\s]+)(\s+\d+)?$",
                line,
            )
            if match:
                name = match.group(1)
                labels_str = match.group(2) or ""
                value = match.group(3)
                base_name = name
                for suffix in ("_total", "_bucket", "_sum", "_count", "_created"):
                    if base_name.endswith(suffix):
                        base_name = base_name[: -len(suffix)]
                        break

                if base_name not in metrics:
                    metrics[base_name] = {"type": current_type, "samples": []}
                try:
                    val = float(value)
                except ValueError:
                    val = value
                metrics[base_name]["samples"].append(
                    {
                        "name": name,
                        "labels": labels_str,
                        "value": val,
                    }
                )
    return metrics


def get_system_metrics_router(grpc_handler, store=None):
    router = APIRouter()

    def _get_prometheus_url() -> str:
        if store:
            fs_cfg = getattr(store.config, "feature_server", None)
            metrics_cfg = getattr(fs_cfg, "metrics", None)
            prom_url = getattr(metrics_cfg, "prometheus_url", None)
            if prom_url:
                return prom_url
        env_url = os.environ.get("FEAST_PROMETHEUS_URL")
        if env_url:
            return env_url
        return _DEFAULT_THANOS_URL

    def _query_prometheus(path: str, params: dict) -> dict:
        prom_url = _get_prometheus_url()
        url = f"{prom_url}{path}"

        headers = {}
        token = _read_sa_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        ca_bundle = _get_ca_bundle()
        verify = ca_bundle if ca_bundle else False

        try:
            resp = http_requests.get(
                url, params=params, headers=headers, verify=verify, timeout=15
            )
            resp.raise_for_status()
            return resp.json()
        except http_requests.exceptions.ConnectionError:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to connect to Prometheus at {prom_url}",
            )
        except http_requests.exceptions.Timeout:
            raise HTTPException(
                status_code=504,
                detail="Failed to query Prometheus: request timed out",
            )
        except http_requests.exceptions.HTTPError as e:
            raise HTTPException(
                status_code=e.response.status_code if e.response else 502,
                detail=f"Failed to query Prometheus: {e}",
            )

    @router.get("/system-metrics/query", tags=["System Metrics"])
    async def promql_instant(
        query: str = Query(..., description="PromQL expression"),
        time: Optional[str] = Query(
            None, description="Evaluation timestamp (RFC3339 or Unix)"
        ),
    ):
        """Proxy a PromQL instant query to Prometheus/Thanos."""
        params: dict = {"query": query}
        if time:
            params["time"] = time
        return _query_prometheus("/api/v1/query", params)

    @router.get("/system-metrics/query_range", tags=["System Metrics"])
    async def promql_range(
        query: str = Query(..., description="PromQL expression"),
        start: str = Query(..., description="Start timestamp"),
        end: str = Query(..., description="End timestamp"),
        step: str = Query("60s", description="Query resolution step"),
    ):
        """Proxy a PromQL range query to Prometheus/Thanos."""
        return _query_prometheus(
            "/api/v1/query_range",
            {
                "query": query,
                "start": start,
                "end": end,
                "step": step,
            },
        )

    @router.get("/system-metrics/scrape", tags=["System Metrics"])
    async def scrape_metrics():
        """Fallback: scrape the local Prometheus metrics endpoint directly."""
        try:
            resp = http_requests.get(
                f"http://localhost:{_METRICS_PORT}/metrics", timeout=5
            )
            resp.raise_for_status()
        except http_requests.exceptions.RequestException:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to scrape local metrics endpoint on port {_METRICS_PORT}",
            )
        return _parse_prometheus_text(resp.text)

    return router
