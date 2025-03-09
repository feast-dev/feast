"""MCP server implementation for Feast feature store."""

import logging
from typing import Optional

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from feast import FeatureStore
from feast.mcp.server import FeastMCP

_logger = logging.getLogger(__name__)


def start_server(
    store: FeatureStore,
    host: str,
    port: int,
    registry_ttl_sec: int = 5,
    tls_cert_path: str = "",
    tls_key_path: str = "",
    enable_auth: bool = False,
    enable_metrics: bool = False,
    cors_origins: Optional[list[str]] = None,
) -> None:
    """Start the MCP server.

    Args:
        store: The FeatureStore to serve
        host: Host to bind to
        port: Port to bind to
        registry_ttl_sec: Time to live for registry cache in seconds
        tls_cert_path: Path to TLS certificate file
        tls_key_path: Path to TLS key file
        enable_auth: Whether to enable authentication
        enable_metrics: Whether to enable metrics
        cors_origins: List of allowed CORS origins
    """
    app = FastAPI(title="Feast MCP Server")
    # Configure CORS
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    # Create MCP server
    mcp_server = FeastMCP(
        feature_store=store,
        name=f"Feast MCP Server ({store.project})",
    )
    # Mount MCP server
    app.mount("/mcp", mcp_server.app)
    # Add health check endpoint
    @app.get("/health")
    def health_check():
        return {"status": "ok"}
    # Start server
    _logger.info(f"Starting MCP server at {host}:{port}")
    if tls_cert_path and tls_key_path:
        uvicorn.run(
            app,
            host=host,
            port=port,
            ssl_certfile=tls_cert_path,
            ssl_keyfile=tls_key_path,
        )
    else:
        uvicorn.run(
            app,
            host=host,
            port=port,
        )
