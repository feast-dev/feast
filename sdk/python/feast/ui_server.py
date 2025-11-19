import json
import threading
from importlib import resources as importlib_resources
from typing import Callable, Optional

import uvicorn
from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import feast


class SaveDocumentRequest(BaseModel):
    file_path: str
    data: dict


def get_app(
    store: "feast.FeatureStore",
    project_id: str,
    registry_ttl_secs: int,
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

    # Asynchronously refresh registry, notifying shutdown and canceling the active timer if the app is shutting down
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

    ui_dir_ref = importlib_resources.files(__spec__.parent) / "ui/build/"  # type: ignore[name-defined, arg-type]
    with importlib_resources.as_file(ui_dir_ref) as ui_dir:
        # Initialize with the projects-list.json file
        with ui_dir.joinpath("projects-list.json").open(mode="w") as f:
            # Get all projects from the registry
            discovered_projects = []
            registry = store.registry.proto()

            # Use the projects list from the registry
            if registry and registry.projects and len(registry.projects) > 0:
                for proj in registry.projects:
                    if proj.spec and proj.spec.name:
                        discovered_projects.append(
                            {
                                "name": proj.spec.name.replace("_", " ").title(),
                                "description": proj.spec.description
                                or f"Project: {proj.spec.name}",
                                "id": proj.spec.name,
                                "registryPath": f"{root_path}/registry",
                            }
                        )
            else:
                # If no projects in registry, use the current project from feature_store.yaml
                discovered_projects.append(
                    {
                        "name": "Project",
                        "description": "Test project",
                        "id": project_id,
                        "registryPath": f"{root_path}/registry",
                    }
                )

            # Add "All Projects" option at the beginning if there are multiple projects
            if len(discovered_projects) > 1:
                all_projects_entry = {
                    "name": "All Projects",
                    "description": "View data across all projects",
                    "id": "all",
                    "registryPath": f"{root_path}/registry",
                }
                discovered_projects.insert(0, all_projects_entry)

            projects_dict = {"projects": discovered_projects}
            f.write(json.dumps(projects_dict))

    @app.get("/registry")
    def read_registry():
        if registry_proto is None:
            return Response(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE
            )  # Service Unavailable
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

    @app.post("/save-document")
    async def save_document_endpoint(request: SaveDocumentRequest):
        try:
            import os
            from pathlib import Path

            file_path = Path(request.file_path).resolve()
            if not str(file_path).startswith(os.getcwd()):
                return {"error": "Invalid file path"}

            base_name = file_path.stem
            labels_file = file_path.parent / f"{base_name}-labels.json"

            with open(labels_file, "w", encoding="utf-8") as file:
                json.dump(request.data, file, indent=2, ensure_ascii=False)

            return {"success": True, "saved_to": str(labels_file)}
        except Exception as e:
            return {"error": str(e)}

    # For all other paths (such as paths that would otherwise be handled by react router), pass to React
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
):
    app = get_app(
        store,
        project_id,
        registry_ttl_sec,
        root_path,
    )
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
