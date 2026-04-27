import json
import logging
import threading
from importlib import resources as importlib_resources
from typing import Callable, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import feast

_logger = logging.getLogger(__name__)


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

    @app.get("/api/mlflow-runs")
    def get_mlflow_runs(max_results: int = 50):
        """Return MLflow runs linked to this Feast project via auto-logging."""
        mlflow_cfg = getattr(store.config, "mlflow", None)
        if not mlflow_cfg or not mlflow_cfg.enabled:
            return {"runs": [], "mlflow_uri": None}

        try:
            import mlflow

            tracking_uri = mlflow_cfg.get_tracking_uri()
            mlflow_ui_base = tracking_uri or mlflow.get_tracking_uri() or ""
            client = mlflow.MlflowClient(tracking_uri=tracking_uri)

            project_name = store.config.project
            experiment = client.get_experiment_by_name(project_name)
            if experiment is None:
                return {"runs": [], "mlflow_uri": mlflow_ui_base or None}
            experiment_ids = [experiment.experiment_id]

            filter_str = (
                f"tags.`feast.project` = '{project_name}' "
                f"AND tags.`feast.retrieval_type` != ''"
            )

            max_results = min(max(max_results, 1), 200)
            runs = client.search_runs(
                experiment_ids=experiment_ids,
                filter_string=filter_str,
                max_results=max_results,
                order_by=["start_time DESC"],
            )

            run_id_to_models: Dict[str, List[dict]] = {}
            try:
                for rm in client.search_registered_models():
                    for mv in rm.latest_versions or []:
                        if mv.run_id:
                            run_id_to_models.setdefault(mv.run_id, []).append(
                                {
                                    "model_name": rm.name,
                                    "version": mv.version,
                                    "stage": mv.current_stage,
                                    "mlflow_url": (
                                        f"{mlflow_ui_base}/#/models/"
                                        f"{rm.name}/versions/{mv.version}"
                                    ),
                                }
                            )
            except Exception:
                pass

            result = []
            for run in runs:
                run_tags = run.data.tags
                run_params = run.data.params
                fv_raw = run_tags.get("feast.feature_views", "")
                refs_raw = run_tags.get(
                    "feast.feature_refs",
                    run_params.get("feast.feature_refs", ""),
                )
                result.append(
                    {
                        "run_id": run.info.run_id,
                        "run_name": run.info.run_name,
                        "status": run.info.status,
                        "start_time": run.info.start_time,
                        "feature_service": run_tags.get("feast.feature_service"),
                        "feature_views": [v for v in fv_raw.split(",") if v],
                        "feature_refs": [v for v in refs_raw.split(",") if v],
                        "retrieval_type": run_tags.get("feast.retrieval_type"),
                        "entity_count": run_tags.get(
                            "feast.entity_count",
                            run_params.get("feast.entity_count"),
                        ),
                        "mlflow_url": (
                            f"{mlflow_ui_base}/#/experiments/"
                            f"{run.info.experiment_id}/runs/{run.info.run_id}"
                        ),
                        "registered_models": run_id_to_models.get(run.info.run_id, []),
                    }
                )

            return {"runs": result, "mlflow_uri": mlflow_ui_base or None}
        except ImportError:
            return {
                "runs": [],
                "mlflow_uri": None,
                "error": "mlflow is not installed",
            }
        except Exception:
            return {
                "runs": [],
                "mlflow_uri": None,
                "error": "Failed to fetch MLflow runs",
            }

    _feature_usage_cache: Dict = {"data": None, "timestamp": 0.0}
    _FEATURE_USAGE_TTL_SECONDS = 300

    @app.get("/api/mlflow-feature-usage")
    def get_mlflow_feature_usage():
        """Return per-feature-view usage stats aggregated from MLflow runs.

        Caches results for 5 minutes to avoid hammering the MLflow server.
        """
        import time as _time

        mlflow_cfg = getattr(store.config, "mlflow", None)
        if not mlflow_cfg or not mlflow_cfg.enabled:
            return {"feature_usage": {}}

        now = _time.monotonic()
        if (
            _feature_usage_cache["data"] is not None
            and (now - _feature_usage_cache["timestamp"]) < _FEATURE_USAGE_TTL_SECONDS
        ):
            return _feature_usage_cache["data"]

        try:
            import mlflow

            tracking_uri = mlflow_cfg.get_tracking_uri()
            client = mlflow.MlflowClient(tracking_uri=tracking_uri)
            project_name = store.config.project

            experiment = client.get_experiment_by_name(project_name)
            if experiment is None:
                result = {"feature_usage": {}}
                _feature_usage_cache["data"] = result
                _feature_usage_cache["timestamp"] = now
                return result

            filter_str = (
                f"tags.`feast.project` = '{project_name}' "
                f"AND tags.`feast.retrieval_type` != ''"
            )
            runs = client.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=filter_str,
                max_results=200,
                order_by=["start_time DESC"],
            )

            run_id_to_models: Dict[str, List[str]] = {}
            try:
                for rm in client.search_registered_models():
                    for mv in rm.latest_versions or []:
                        if mv.run_id:
                            run_id_to_models.setdefault(mv.run_id, []).append(rm.name)
            except Exception:
                pass

            usage: Dict[str, dict] = {}
            for run in runs:
                refs_raw = run.data.tags.get("feast.feature_refs", "")
                fv_names = set()
                for ref in refs_raw.split(","):
                    ref = ref.strip()
                    if ":" in ref:
                        fv_names.add(ref.split(":")[0])

                run_models = run_id_to_models.get(run.info.run_id, [])

                for fv_name in fv_names:
                    if fv_name not in usage:
                        usage[fv_name] = {
                            "run_count": 0,
                            "last_used": None,
                            "models": [],
                        }
                    usage[fv_name]["run_count"] += 1
                    run_ts = run.info.start_time
                    if usage[fv_name]["last_used"] is None or (
                        run_ts and run_ts > usage[fv_name]["last_used"]
                    ):
                        usage[fv_name]["last_used"] = run_ts
                    for m in run_models:
                        if m not in usage[fv_name]["models"]:
                            usage[fv_name]["models"].append(m)

            result = {"feature_usage": usage}
            _feature_usage_cache["data"] = result
            _feature_usage_cache["timestamp"] = now
            return result
        except ImportError:
            return {"feature_usage": {}, "error": "mlflow is not installed"}
        except Exception as e:
            _logger.debug("Failed to fetch feature usage: %s", e)
            return {"feature_usage": {}, "error": "Failed to fetch usage data"}

    @app.get("/api/mlflow-feature-models")
    def get_mlflow_feature_models():
        """Return a mapping of feature_ref -> registered models that use it.

        Walks the MLflow Model Registry, inspects the training run for each
        model's latest version(s), reads the ``feast.feature_refs`` tag, and
        inverts it into a reverse index so the UI can show which registered
        models depend on a given feature.
        """
        mlflow_cfg = getattr(store.config, "mlflow", None)
        if not mlflow_cfg or not mlflow_cfg.enabled:
            return {"feature_models": {}}

        try:
            import mlflow

            tracking_uri = mlflow_cfg.get_tracking_uri()
            mlflow_ui_base = tracking_uri or mlflow.get_tracking_uri() or ""
            client = mlflow.MlflowClient(tracking_uri=tracking_uri)
            project_name = store.config.project

            feature_models: Dict[str, List[dict]] = {}

            for rm in client.search_registered_models():
                model_name = rm.name
                latest_versions = rm.latest_versions or []
                for mv in latest_versions:
                    if not mv.run_id:
                        continue
                    try:
                        run = client.get_run(mv.run_id)
                    except Exception:
                        continue

                    tags = run.data.tags
                    if tags.get("feast.project") != project_name:
                        continue

                    refs_raw = tags.get("feast.feature_refs", "")
                    feature_refs = [r for r in refs_raw.split(",") if r]

                    model_info = {
                        "model_name": model_name,
                        "version": mv.version,
                        "stage": mv.current_stage,
                        "mlflow_url": (
                            f"{mlflow_ui_base}/#/models/"
                            f"{model_name}/versions/{mv.version}"
                        ),
                    }

                    for ref in feature_refs:
                        feature_models.setdefault(ref, []).append(model_info)

            return {"feature_models": feature_models}
        except ImportError:
            return {
                "feature_models": {},
                "error": "mlflow is not installed",
            }
        except Exception as e:
            _logger.debug("Failed to fetch MLflow feature-model mapping: %s", e)
            return {
                "feature_models": {},
                "error": "Failed to fetch model data",
            }

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
