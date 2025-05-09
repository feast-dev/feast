import logging

from fastapi import Depends, FastAPI, status

from feast import FeatureStore
from feast.api.registry.rest import register_all_routes
from feast.permissions.auth.auth_manager import get_auth_manager
from feast.permissions.server.rest import inject_user_details
from feast.permissions.server.utils import (
    ServerType,
    init_auth_manager,
    init_security_manager,
    str_to_auth_manager_type,
)
from feast.registry_server import RegistryServer

logger = logging.getLogger(__name__)


class RestRegistryServer:
    def __init__(self, store: FeatureStore):
        self.store = store
        self.registry = store.registry
        self.grpc_handler = RegistryServer(self.registry)
        self.app = FastAPI(
            title="Feast REST Registry Server",
            description="Feast REST Registry Server",
            dependencies=[Depends(inject_user_details)],
            version="1.0.0",
            openapi_url="/openapi.json",
            docs_url="/",
            redoc_url="/docs",
            default_status_code=status.HTTP_200_OK,
            default_headers={
                "X-Content-Type-Options": "nosniff",
                "X-XSS-Protection": "1; mode=block",
                "X-Frame-Options": "DENY",
            },
        )
        self._add_openapi_security()
        self._init_auth()
        self._register_routes()

    def _add_openapi_security(self):
        if self.app.openapi_schema:
            return
        original_openapi = self.app.openapi

        def custom_openapi():
            if self.app.openapi_schema:
                return self.app.openapi_schema
            schema = original_openapi()
            schema.setdefault("components", {}).setdefault("securitySchemes", {})[
                "BearerAuth"
            ] = {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT",
            }
            schema.setdefault("security", []).append({"BearerAuth": []})
            self.app.openapi_schema = schema
            return self.app.openapi_schema

        self.app.openapi = custom_openapi

    def _init_auth(self):
        auth_type = str_to_auth_manager_type(self.store.config.auth_config.type)
        init_security_manager(auth_type=auth_type, fs=self.store)
        init_auth_manager(
            auth_type=auth_type,
            server_type=ServerType.REST,
            auth_config=self.store.config.auth_config,
        )
        self.auth_manager = get_auth_manager()

    def _register_routes(self):
        register_all_routes(self.app, self.grpc_handler)

    def start_server(
        self,
        port: int,
        tls_key_path: str = "",
        tls_cert_path: str = "",
    ):
        import uvicorn

        if tls_key_path and tls_cert_path:
            logger.info("Starting REST registry server in TLS(SSL) mode")
            print(f"REST registry server listening on https://localhost:{port}")
            uvicorn.run(
                self.app,
                host="0.0.0.0",
                port=port,
                ssl_keyfile=tls_key_path,
                ssl_certfile=tls_cert_path,
            )
        else:
            print("Starting REST registry server in non-TLS(SSL) mode")
            print(f"REST registry server listening on http://localhost:{port}")
            uvicorn.run(
                self.app,
                host="0.0.0.0",
                port=port,
            )
