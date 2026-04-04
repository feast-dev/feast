import json
import logging
import threading
from importlib import resources as importlib_resources
from typing import Callable, Optional

import uvicorn
from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import feast

logger = logging.getLogger(__name__)


def _build_projects_list(
    store: "feast.FeatureStore",
    project_id: str,
    root_path: str,
    mode: str,
):
    """Build the projects list for the UI, with mode-aware registry paths."""
    discovered_projects = []
    registry = store.registry.proto()

    if mode == "proto":
        registry_path_template = f"{root_path}/registry"
    else:
        registry_path_template = f"{root_path}/api/v1"

    if registry and registry.projects and len(registry.projects) > 0:
        for proj in registry.projects:
            if proj.spec and proj.spec.name:
                discovered_projects.append(
                    {
                        "name": proj.spec.name.replace("_", " ").title(),
                        "description": proj.spec.description
                        or f"Project: {proj.spec.name}",
                        "id": proj.spec.name,
                        "registryPath": registry_path_template,
                    }
                )
    else:
        discovered_projects.append(
            {
                "name": "Project",
                "description": "Test project",
                "id": project_id,
                "registryPath": registry_path_template,
            }
        )

    if len(discovered_projects) > 1:
        all_projects_entry = {
            "name": "All Projects",
            "description": "View data across all projects",
            "id": "all",
            "registryPath": registry_path_template,
        }
        discovered_projects.insert(0, all_projects_entry)

    return {"projects": discovered_projects, "mode": mode}


def _setup_proto_mode(
    app: FastAPI, store: "feast.FeatureStore", registry_ttl_secs: int
):
    """Set up the legacy proto-blob serving mode (GET /registry)."""
    registry_proto = None
    shutting_down = False
    active_timer: Optional[threading.Timer] = None

    def async_refresh():
        store.refresh_registry()
        nonlocal registry_proto
        registry_proto = store.registry.proto()
        if shutting_down:
            return
        nonlocal active_timer
        active_timer = threading.Timer(registry_ttl_secs, async_refresh)
        active_timer.start()

    @app.on_event("shutdown")
    def shutdown_event():
        nonlocal shutting_down
        shutting_down = True
        if active_timer:
            active_timer.cancel()

    async_refresh()

    @app.get("/registry")
    def read_registry():
        if registry_proto is None:
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        return Response(
            content=registry_proto.SerializeToString(),
            media_type="application/octet-stream",
        )

    @app.get("/health")
    def health():
        return (
            Response(status_code=status.HTTP_200_OK)
            if registry_proto
            else Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        )


def _setup_rest_mode(app: FastAPI, store: "feast.FeatureStore", registry_ttl_secs: int):
    """Mount the REST registry API routes on the UI server under /api/v1."""
    from feast.api.registry.rest import register_all_routes
    from feast.registry_server import RegistryServer

    registry_proto = None
    shutting_down = False
    active_timer: Optional[threading.Timer] = None

    def async_refresh():
        store.refresh_registry()
        nonlocal registry_proto
        registry_proto = store.registry.proto()
        if shutting_down:
            return
        nonlocal active_timer
        active_timer = threading.Timer(registry_ttl_secs, async_refresh)
        active_timer.start()

    @app.on_event("shutdown")
    def shutdown_event():
        nonlocal shutting_down
        shutting_down = True
        if active_timer:
            active_timer.cancel()

    async_refresh()

    grpc_handler = RegistryServer(store.registry)

    rest_app = FastAPI(root_path="/api/v1")
    register_all_routes(rest_app, grpc_handler)
    app.mount("/api/v1", rest_app)

    @app.get("/registry")
    def read_registry():
        if registry_proto is None:
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        return Response(
            content=registry_proto.SerializeToString(),
            media_type="application/octet-stream",
        )

    @app.get("/health")
    def health():
        return (
            Response(status_code=status.HTTP_200_OK)
            if registry_proto
            else Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        )

    logger.info("REST registry API mounted at /api/v1")


def _setup_rest_external_mode(
    app: FastAPI,
    store: "feast.FeatureStore",
    rest_api_url: str,
    registry_ttl_secs: int,
):
    """Reverse-proxy REST API calls to an external registry server."""
    import httpx

    rest_api_url = rest_api_url.rstrip("/")
    client = httpx.AsyncClient(timeout=60.0)

    registry_proto = None
    shutting_down = False
    active_timer: Optional[threading.Timer] = None

    def async_refresh():
        store.refresh_registry()
        nonlocal registry_proto
        registry_proto = store.registry.proto()
        if shutting_down:
            return
        nonlocal active_timer
        active_timer = threading.Timer(registry_ttl_secs, async_refresh)
        active_timer.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        nonlocal shutting_down
        shutting_down = True
        if active_timer:
            active_timer.cancel()
        await client.aclose()

    async_refresh()

    @app.api_route("/api/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
    async def proxy_to_external(request: Request, path: str):
        target_url = f"{rest_api_url}/{path}"
        query_string = str(request.url.query)
        if query_string:
            target_url = f"{target_url}?{query_string}"

        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in ("host", "content-length", "transfer-encoding")
        }

        body = await request.body()

        try:
            resp = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=body if body else None,
            )
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                media_type=resp.headers.get("content-type", "application/json"),
            )
        except httpx.RequestError as e:
            logger.error(f"Error proxying to {target_url}: {e}")
            return Response(
                content=json.dumps(
                    {"detail": "Failed to reach the upstream registry API"}
                ),
                status_code=status.HTTP_502_BAD_GATEWAY,
                media_type="application/json",
            )

    @app.get("/registry")
    def read_registry():
        if registry_proto is None:
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        return Response(
            content=registry_proto.SerializeToString(),
            media_type="application/octet-stream",
        )

    @app.get("/health")
    def health():
        return (
            Response(status_code=status.HTTP_200_OK)
            if registry_proto
            else Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
        )

    logger.info(f"REST external proxy configured → {rest_api_url}")


def get_app(
    store: "feast.FeatureStore",
    project_id: str,
    registry_ttl_secs: int,
    root_path: str = "",
    mode: str = "proto",
    rest_api_url: str = "",
):
    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if mode == "rest":
        _setup_rest_mode(app, store, registry_ttl_secs)
    elif mode == "rest-external":
        _setup_rest_external_mode(app, store, rest_api_url, registry_ttl_secs)
    else:
        _setup_proto_mode(app, store, registry_ttl_secs)

    ui_dir_ref = importlib_resources.files(__spec__.parent) / "ui/build/"  # type: ignore[name-defined, arg-type]
    with importlib_resources.as_file(ui_dir_ref) as ui_dir:
        projects_dict = _build_projects_list(store, project_id, root_path, mode)
        with ui_dir.joinpath("projects-list.json").open(mode="w") as f:
            f.write(json.dumps(projects_dict))

    @app.api_route("/p/{path_name:path}", methods=["GET"])
    def catch_all():
        filename = ui_dir.joinpath("index.html")
        with open(filename) as f:
            content = f.read()
        return Response(content, media_type="text/html")

    app.mount(
        "/",
        StaticFiles(directory=ui_dir, html=True),
        name="site",
    )

    return app


def start_server(
    store: "feast.FeatureStore",
    host: str,
    port: int,
    get_registry_dump: Callable,
    project_id: str,
    registry_ttl_sec: int,
    root_path: str = "",
    tls_key_path: str = "",
    tls_cert_path: str = "",
    mode: str = "proto",
    rest_api_url: str = "",
):
    app = get_app(
        store,
        project_id,
        registry_ttl_sec,
        root_path,
        mode=mode,
        rest_api_url=rest_api_url,
    )

    logger.info(f"Starting Feast UI server in '{mode}' mode on {host}:{port}")

    if tls_key_path and tls_cert_path:
        uvicorn.run(
            app,
            host=host,
            port=port,
            ssl_keyfile=tls_key_path,
            ssl_certfile=tls_cert_path,
        )
    else:
        uvicorn.run(app, host=host, port=port)
