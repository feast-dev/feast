import json
import logging
import re

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from feast import FeatureStore
from feast.api.registry.rest import register_all_routes
from feast.errors import (
    FeastObjectNotFoundException,
    FeastPermissionError,
    PushSourceNotFoundException,
)
from feast.permissions.auth.auth_manager import get_auth_manager
from feast.permissions.server.rest import inject_user_details
from feast.permissions.server.utils import (
    ServerType,
    init_auth_manager,
    init_security_manager,
    str_to_auth_manager_type,
)
from feast.registry_server import RegistryServer
from feast.utils import _utc_now

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RestRegistryServer:
    def __init__(self, store: FeatureStore):
        self.store = store
        self.registry = store.registry
        self.grpc_handler = RegistryServer(self.registry)
        recent_visit_logging_cfg = {}
        feature_server_cfg = getattr(
            getattr(store, "config", None), "feature_server", None
        )
        if isinstance(feature_server_cfg, dict):
            recent_visit_logging_cfg = feature_server_cfg.get(
                "recent_visit_logging", {}
            )
        self.recent_visits_limit = recent_visit_logging_cfg.get("limit", 100)
        self.log_patterns = recent_visit_logging_cfg.get(
            "log_patterns",
            [
                r".*/entities/(?!all$)[^/]+$",
                r".*/data_sources/(?!all$)[^/]+$",
                r".*/feature_views/(?!all$)[^/]+$",
                r".*/features/(?!all$)[^/]+$",
                r".*/feature_services/(?!all$)[^/]+$",
                r".*/saved_datasets/(?!all$)[^/]+$",
            ],
        )
        self.app = FastAPI(
            title="Feast REST Registry Server",
            description="Feast REST Registry Server",
            dependencies=[Depends(inject_user_details)],
            version="1.0.0",
            openapi_url="/openapi.json",
            root_path="/api/v1",
            docs_url="/",
            redoc_url="/docs",
            default_status_code=status.HTTP_200_OK,
            default_headers={
                "X-Content-Type-Options": "nosniff",
                "X-XSS-Protection": "1; mode=block",
                "X-Frame-Options": "DENY",
            },
        )
        self._add_exception_handlers()
        self._add_logging_middleware()
        self._add_openapi_security()
        self._init_auth()
        self._register_routes()

    def _add_exception_handlers(self):
        """Add custom exception handlers to include HTTP status codes in JSON responses."""

        @self.app.exception_handler(HTTPException)
        async def http_exception_handler(request: Request, exc: HTTPException):
            return JSONResponse(
                status_code=exc.status_code,
                content={
                    "status_code": exc.status_code,
                    "detail": exc.detail,
                    "error_type": "HTTPException",
                },
            )

        @self.app.exception_handler(FeastObjectNotFoundException)
        async def feast_object_not_found_handler(
            request: Request, exc: FeastObjectNotFoundException
        ):
            return JSONResponse(
                status_code=404,
                content={
                    "status_code": 404,
                    "detail": str(exc),
                    "error_type": "FeastObjectNotFoundException",
                },
            )

        @self.app.exception_handler(FeastPermissionError)
        async def feast_permission_error_handler(
            request: Request, exc: FeastPermissionError
        ):
            return JSONResponse(
                status_code=403,
                content={
                    "status_code": 403,
                    "detail": str(exc),
                    "error_type": "FeastPermissionError",
                },
            )

        @self.app.exception_handler(PushSourceNotFoundException)
        async def push_source_not_found_handler(
            request: Request, exc: PushSourceNotFoundException
        ):
            return JSONResponse(
                status_code=422,
                content={
                    "status_code": 422,
                    "detail": str(exc),
                    "error_type": "PushSourceNotFoundException",
                },
            )

        @self.app.exception_handler(ValidationError)
        async def validation_error_handler(request: Request, exc: ValidationError):
            return JSONResponse(
                status_code=422,
                content={
                    "status_code": 422,
                    "detail": str(exc),
                    "error_type": "ValidationError",
                },
            )

        @self.app.exception_handler(RequestValidationError)
        async def request_validation_error_handler(
            request: Request, exc: RequestValidationError
        ):
            return JSONResponse(
                status_code=422,
                content={
                    "status_code": 422,
                    "detail": str(exc),
                    "error_type": "RequestValidationError",
                },
            )

        @self.app.exception_handler(ValueError)
        async def value_error_handler(request: Request, exc: ValueError):
            return JSONResponse(
                status_code=422,
                content={
                    "status_code": 422,
                    "detail": str(exc),
                    "error_type": "ValueError",
                },
            )

        @self.app.exception_handler(Exception)
        async def generic_exception_handler(request: Request, exc: Exception):
            logger.error(f"Unhandled exception: {exc}", exc_info=True)

            return JSONResponse(
                status_code=500,
                content={
                    "status_code": 500,
                    "detail": f"Internal server error: {str(exc)}",
                    "error_type": "InternalServerError",
                },
            )

    def _add_logging_middleware(self):
        from fastapi import Request
        from starlette.middleware.base import BaseHTTPMiddleware

        class LoggingMiddleware(BaseHTTPMiddleware):
            def __init__(
                self, app, registry, project, recent_visits_limit, log_patterns
            ):
                super().__init__(app)
                self.registry = registry
                self.project = project
                self.recent_visits_limit = recent_visits_limit
                self.log_patterns = [re.compile(p) for p in log_patterns]

            async def dispatch(self, request: Request, call_next):
                LOG_PATTERNS = self.log_patterns
                if request.method == "GET":
                    user = None
                    if hasattr(request.state, "user"):
                        user = getattr(request.state, "user", None)
                    if not user:
                        user = "anonymous"
                    project = request.query_params.get("project") or self.project
                    key = f"recently_visited_{user}"
                    path = str(request.url.path)
                    method = request.method

                    if path.startswith("/api/v1/metrics/"):
                        response = await call_next(request)
                        return response

                    object_type = None
                    object_name = None
                    if not any(p.match(path) for p in LOG_PATTERNS):
                        response = await call_next(request)
                        return response
                    m = re.match(r"/api/v1/([^/]+)(?:/([^/]+))?", path)
                    if m:
                        object_type = m.group(1)
                        object_name = m.group(2)
                    else:
                        object_type = None
                        object_name = None
                    visit = {
                        "path": path,
                        "timestamp": _utc_now().isoformat(),
                        "project": project,
                        "user": user,
                        "object": object_type,
                        "object_name": object_name,
                        "method": method,
                    }
                    try:
                        visits_json = self.registry.get_project_metadata(project, key)
                        visits = json.loads(visits_json) if visits_json else []
                    except Exception:
                        visits = []
                    visits.append(visit)
                    visits = visits[-self.recent_visits_limit :]
                    try:
                        self.registry.set_project_metadata(
                            project, key, json.dumps(visits)
                        )
                    except Exception as e:
                        logger.warning(f"Failed to persist recent visits: {e}")
                response = await call_next(request)
                return response

        self.app.add_middleware(
            LoggingMiddleware,
            registry=self.registry,
            project=self.store.project,
            recent_visits_limit=self.recent_visits_limit,
            log_patterns=self.log_patterns,
        )

    def _register_routes(self):
        register_all_routes(self.app, self.grpc_handler, self)

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

    def start_server(
        self,
        port: int,
        tls_key_path: str = "",
        tls_cert_path: str = "",
    ):
        import uvicorn

        if tls_key_path and tls_cert_path:
            logger.info("Starting REST registry server in TLS(SSL) mode")
            logger.info(f"REST registry server listening on https://localhost:{port}")
            uvicorn.run(
                self.app,
                host="0.0.0.0",
                port=port,
                ssl_keyfile=tls_key_path,
                ssl_certfile=tls_cert_path,
            )
        else:
            logger.info("Starting REST registry server in non-TLS(SSL) mode")
            logger.info(f"REST registry server listening on http://localhost:{port}")
            uvicorn.run(
                self.app,
                host="0.0.0.0",
                port=port,
            )
