import json

import pkg_resources
import uvicorn
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import feast


def get_app(store: "feast.FeatureStore", registry_dump: str, project_id: str):
    ui_dir = pkg_resources.resource_filename(__name__, "ui/build/")

    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/registry")
    def read_registry():
        return json.loads(registry_dump)

    # Generate projects-list json that points to the current repo's project
    # TODO(adchia): Enable users to also add project name + description fields in feature_store.yaml
    @app.get("/projects-list")
    def projects_list():
        projects = {
            "projects": [
                {
                    "name": "Project",
                    "description": "Test project",
                    "id": project_id,
                    "registryPath": "http://0.0.0.0:8888/registry",
                }
            ]
        }
        return projects

    # For all other paths (such as paths that would otherwise be handled by react router), pass to React
    @app.api_route("/p/{path_name:path}", methods=["GET"])
    def catch_all():
        filename = ui_dir + "index.html"

        with open(filename) as f:
            content = f.read()

        return Response(content, media_type="text/html")

    app.mount(
        "/", StaticFiles(directory=ui_dir, html=True), name="site",
    )

    return app


def start_server(store: "feast.FeatureStore", registry_dump: str, project_id: str):
    app = get_app(store, registry_dump, project_id)
    uvicorn.run(app, host="0.0.0.0", port=8888)
