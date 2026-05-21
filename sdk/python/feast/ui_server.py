import json
import logging
from importlib import resources as importlib_resources

import uvicorn
from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import feast

logger = logging.getLogger(__name__)


def _build_projects_list(
    store: "feast.FeatureStore",
    project_id: str,
    root_path: str,
):
    """Build the projects list for the UI."""
    discovered_projects = []
    registry_path_template = f"{root_path}/api/v1"

    try:
        projects = store.registry.list_projects(allow_cache=True)
        for proj in projects:
            discovered_projects.append(
                {
                    "name": proj.name.replace("_", " ").title(),
                    "description": proj.description or f"Project: {proj.name}",
                    "id": proj.name,
                    "registryPath": registry_path_template,
                }
            )
    except Exception:
        pass

    if not discovered_projects:
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

    return {"projects": discovered_projects}


def _setup_rest_mode(app: FastAPI, store: "feast.FeatureStore"):
    """Mount the REST registry API routes on the UI server under /api/v1."""
    from feast.api.registry.rest import register_all_routes
    from feast.registry_server import RegistryServer

    grpc_handler = RegistryServer(store.registry)

    rest_app = FastAPI(root_path="/api/v1")
    register_all_routes(rest_app, grpc_handler)
    app.mount("/api/v1", rest_app)

    @app.get("/health")
    def health():
        try:
            store.registry.list_projects(allow_cache=True)
            return Response(status_code=status.HTTP_200_OK)
        except Exception:
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    logger.info("REST registry API mounted at /api/v1")


def get_app(
    store: "feast.FeatureStore",
    project_id: str,
    root_path: str = "",
):
    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    _setup_rest_mode(app, store)

    ui_dir_ref = importlib_resources.files(__spec__.parent) / "ui/build/"  # type: ignore[name-defined, arg-type]
    with importlib_resources.as_file(ui_dir_ref) as ui_dir:
        projects_dict = _build_projects_list(store, project_id, root_path)
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
    project_id: str,
    root_path: str = "",
    tls_key_path: str = "",
    tls_cert_path: str = "",
):
    app = get_app(
        store,
        project_id,
        root_path,
    )

    logger.info(f"Starting Feast UI server on {host}:{port}")

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
