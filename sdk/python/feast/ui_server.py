import json
import logging
from importlib import resources as importlib_resources
from typing import Any, Dict, List, Optional

import pandas as pd
import uvicorn
from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import feast

logger = logging.getLogger(__name__)


def _safe_error_response(
    operation: str, status_code: int = status.HTTP_400_BAD_REQUEST
) -> Response:
    """Return a generic error response without exposing internal details."""
    return Response(
        content=json.dumps(
            {"detail": f"{operation} failed. Check server logs for details."}
        ),
        status_code=status_code,
        media_type="application/json",
    )


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

    grpc_handler = RegistryServer(store.registry, store=store)

    rest_app = FastAPI(root_path="/api/v1")
    register_all_routes(rest_app, grpc_handler, store=store)

    class PushRequest(BaseModel):
        push_source_name: str
        df: Dict[str, List]
        to: Optional[str] = "online"

    @rest_app.post("/push")
    def push_labels(request: PushRequest):
        """Push label data to a LabelView (or any PushSource-backed FeatureView)."""
        try:
            df = pd.DataFrame(request.df)
            if "event_timestamp" in df.columns:
                df["event_timestamp"] = pd.to_datetime(
                    df["event_timestamp"], utc=True
                ).dt.tz_localize(None)
            to = request.to or "online"
            if to == "online_and_offline":
                store.push(
                    request.push_source_name,
                    df,
                    to=feast.data_source.PushMode.ONLINE_AND_OFFLINE,
                )
            elif to == "offline":
                store.push(
                    request.push_source_name, df, to=feast.data_source.PushMode.OFFLINE
                )
            else:
                store.push(
                    request.push_source_name, df, to=feast.data_source.PushMode.ONLINE
                )
            return {"status": "ok"}
        except Exception:
            logger.exception("Push failed")
            return _safe_error_response("Push")

    class GetOnlineFeaturesRequest(BaseModel):
        feature_view: str
        entity_keys: Dict[str, List]

    @rest_app.post("/get-online-labels")
    def get_online_labels(request: GetOnlineFeaturesRequest):
        """Retrieve current label values from the online store for given entity keys."""
        try:
            fv_name = request.feature_view

            fv: Any = None
            try:
                fv = store.get_feature_view(fv_name)
            except Exception:
                pass

            if fv is None:
                try:
                    fv = store.registry.get_label_view(fv_name, store.project)
                except Exception:
                    pass

            if fv is None:
                return Response(
                    content=json.dumps(
                        {"detail": f"Feature view or label view '{fv_name}' not found"}
                    ),
                    status_code=status.HTTP_404_NOT_FOUND,
                    media_type="application/json",
                )

            feature_refs = [f"{fv_name}:{f.name}" for f in fv.features]

            result = store.get_online_features(
                features=feature_refs,
                entity_rows=[
                    {k: v[i] for k, v in request.entity_keys.items()}
                    for i in range(len(next(iter(request.entity_keys.values()))))
                ],
            )

            result_dict = result.to_dict()
            return {"results": result_dict}
        except Exception:
            logger.exception("get-online-labels failed")
            return _safe_error_response("Get online labels")

    class ListLabelsRequest(BaseModel):
        feature_view: str
        limit: Optional[int] = 100

    @rest_app.post("/list-labels")
    def list_labels(request: ListLabelsRequest):
        """List resolved labels from the offline store with conflict policy enforcement."""
        try:
            from feast.labeling.conflict_policy import ConflictPolicy
            from feast.labeling.conflict_resolver import resolve_conflicts

            fv_name = request.feature_view
            fv: Any = None
            try:
                fv = store.registry.get_label_view(fv_name, store.project)
            except Exception:
                try:
                    fv = store.get_feature_view(fv_name)
                except Exception:
                    pass

            if fv is None:
                return Response(
                    content=json.dumps({"detail": f"Label view '{fv_name}' not found"}),
                    status_code=status.HTTP_404_NOT_FOUND,
                    media_type="application/json",
                )

            batch_source = getattr(fv, "batch_source", None)
            if batch_source is None:
                return Response(
                    content=json.dumps({"detail": f"No batch source for '{fv_name}'"}),
                    status_code=status.HTTP_400_BAD_REQUEST,
                    media_type="application/json",
                )

            feature_names = [f.name for f in fv.features]
            join_keys = (
                fv.join_keys
                if hasattr(fv, "join_keys")
                else (fv.entities if fv.entities else [])
            )

            provider = store._get_provider()
            timestamp_field = batch_source.timestamp_field

            conflict_policy = getattr(
                fv, "conflict_policy", ConflictPolicy.LAST_WRITE_WINS
            )
            labeler_field = getattr(fv, "labeler_field", "labeler")

            try:
                job = provider.offline_store.pull_all_from_table_or_query(
                    config=store.config,
                    data_source=batch_source,
                    join_key_columns=join_keys,
                    feature_name_columns=feature_names,
                    timestamp_field=timestamp_field,
                )
                df = job.to_df()
            except Exception:
                return {
                    "labels": [],
                    "columns": join_keys + feature_names,
                    "feature_names": feature_names,
                    "entity_names": join_keys,
                    "total_count": 0,
                    "total_entities": 0,
                    "conflict_policy": conflict_policy.value,
                    "hint": "No label data found yet. Push labels to get started.",
                }

            df = resolve_conflicts(
                df=df,
                join_key_columns=join_keys,
                feature_name_columns=feature_names,
                timestamp_field=timestamp_field,
                labeler_field=labeler_field,
                conflict_policy=conflict_policy,
            )

            limit = request.limit or 100
            df = df.head(limit)

            result = df.to_dict(orient="records")
            for row in result:
                for k, v in row.items():
                    if pd.isna(v):
                        row[k] = None
                    elif hasattr(v, "isoformat"):
                        row[k] = v.isoformat()

            return {
                "labels": result,
                "columns": list(df.columns),
                "feature_names": feature_names,
                "entity_names": join_keys,
                "total_count": len(result),
                "total_entities": len(result),
                "conflict_policy": conflict_policy.value,
            }
        except Exception:
            logger.exception("list-labels failed")
            return _safe_error_response("List labels")

    class LabelQualityRequest(BaseModel):
        feature_view: str

    @rest_app.post("/label-quality")
    def label_quality(request: LabelQualityRequest):
        """Compute label quality metrics with conflict policy enforcement."""
        try:
            from datetime import datetime, timezone

            from feast.labeling.conflict_policy import ConflictPolicy
            from feast.labeling.conflict_resolver import resolve_conflicts

            fv_name = request.feature_view
            fv: Any = None
            try:
                fv = store.registry.get_label_view(fv_name, store.project)
            except Exception:
                try:
                    fv = store.get_feature_view(fv_name)
                except Exception:
                    pass

            if fv is None:
                return Response(
                    content=json.dumps({"detail": f"Label view '{fv_name}' not found"}),
                    status_code=status.HTTP_404_NOT_FOUND,
                    media_type="application/json",
                )

            batch_source = getattr(fv, "batch_source", None)
            if batch_source is None:
                return Response(
                    content=json.dumps({"detail": f"No batch source for '{fv_name}'"}),
                    status_code=status.HTTP_400_BAD_REQUEST,
                    media_type="application/json",
                )

            feature_names = [f.name for f in fv.features]
            join_keys = (
                fv.join_keys
                if hasattr(fv, "join_keys")
                else (fv.entities if fv.entities else [])
            )

            provider = store._get_provider()
            timestamp_field = batch_source.timestamp_field
            labeler_field = getattr(fv, "labeler_field", "labeler")
            conflict_policy = getattr(
                fv, "conflict_policy", ConflictPolicy.LAST_WRITE_WINS
            )

            try:
                job = provider.offline_store.pull_all_from_table_or_query(
                    config=store.config,
                    data_source=batch_source,
                    join_key_columns=join_keys,
                    feature_name_columns=feature_names,
                    timestamp_field=timestamp_field,
                )
                raw_df = job.to_df()
            except Exception:
                return {
                    "total_entities": 0,
                    "total_annotations": 0,
                    "feature_names": feature_names,
                    "distributions": {},
                    "coverage_pct": {},
                    "null_counts": {},
                    "labeler_stats": {},
                    "staleness_seconds": None,
                    "oldest_label_ts": None,
                    "newest_label_ts": None,
                    "labeler_field": labeler_field,
                    "conflict_policy": conflict_policy.value,
                    "hint": "No label data found yet. Push labels to compute quality metrics.",
                }

            total_annotations = len(raw_df)

            # Labeler stats computed on raw data (before resolution)
            labeler_stats: Dict[str, int] = {}
            if labeler_field and labeler_field in raw_df.columns:
                labeler_stats = (
                    raw_df[labeler_field].dropna().astype(str).value_counts().to_dict()
                )

            # Resolve conflicts to get one row per entity
            df = resolve_conflicts(
                df=raw_df,
                join_key_columns=join_keys,
                feature_name_columns=feature_names,
                timestamp_field=timestamp_field,
                labeler_field=labeler_field,
                conflict_policy=conflict_policy,
            )

            total_entities = len(df)
            ts_col = timestamp_field

            staleness_seconds = None
            oldest_ts = None
            newest_ts = None
            if ts_col in df.columns and not df[ts_col].empty:
                ts_series = pd.to_datetime(df[ts_col], utc=True)
                oldest_ts = ts_series.min().isoformat()
                newest_ts = ts_series.max().isoformat()
                staleness_seconds = (
                    datetime.now(timezone.utc) - ts_series.max()
                ).total_seconds()

            distributions: Dict[str, Dict[str, int]] = {}
            coverage: Dict[str, float] = {}
            null_counts: Dict[str, int] = {}

            for fn in feature_names:
                if fn in df.columns:
                    col = df[fn]
                    nulls = int(col.isna().sum())
                    total = len(col)
                    null_counts[fn] = nulls
                    coverage[fn] = ((total - nulls) / total * 100) if total > 0 else 0.0
                    non_null = col.dropna()
                    distributions[fn] = (
                        non_null.astype(str).value_counts().head(20).to_dict()
                    )
                else:
                    null_counts[fn] = total_entities
                    coverage[fn] = 0.0
                    distributions[fn] = {}

            return {
                "total_entities": total_entities,
                "total_annotations": total_annotations,
                "feature_names": feature_names,
                "distributions": distributions,
                "coverage_pct": coverage,
                "null_counts": null_counts,
                "labeler_stats": labeler_stats,
                "staleness_seconds": staleness_seconds,
                "oldest_label_ts": oldest_ts,
                "newest_label_ts": newest_ts,
                "labeler_field": labeler_field,
                "conflict_policy": conflict_policy.value,
            }
        except Exception:
            logger.exception("Label quality failed")
            return _safe_error_response("Label quality")

    # --- Active Learning Endpoint (offline-store-based, backend-agnostic) ---

    class ActiveLearningRequest(BaseModel):
        feature_view: str
        reference_feature_view: Optional[str] = None
        limit: Optional[int] = 50

    @rest_app.post("/active-learning/candidates")
    def active_learning_candidates(request: ActiveLearningRequest):
        """Find entities that exist in a reference feature view but have NOT been labeled yet."""
        try:
            fv_name = request.feature_view
            fv: Any = None
            try:
                fv = store.registry.get_label_view(fv_name, store.project)
            except Exception:
                pass

            if fv is None:
                return Response(
                    content=json.dumps({"detail": f"Label view '{fv_name}' not found"}),
                    status_code=status.HTTP_404_NOT_FOUND,
                    media_type="application/json",
                )

            ref_fv_name = request.reference_feature_view or (
                getattr(fv, "reference_feature_view", None) or ""
            )

            if not ref_fv_name:
                return Response(
                    content=json.dumps(
                        {
                            "detail": "No reference feature view specified. "
                            "Provide a feature view containing all entities."
                        }
                    ),
                    status_code=status.HTTP_400_BAD_REQUEST,
                    media_type="application/json",
                )

            ref_fv: Any = None
            try:
                ref_fv = store.get_feature_view(ref_fv_name)
            except Exception:
                pass

            if ref_fv is None:
                return Response(
                    content=json.dumps(
                        {"detail": f"Reference feature view '{ref_fv_name}' not found"}
                    ),
                    status_code=status.HTTP_404_NOT_FOUND,
                    media_type="application/json",
                )

            provider = store._get_provider()

            # Get all entities from reference feature view
            ref_batch_source = getattr(ref_fv, "batch_source", None)
            if ref_batch_source is None:
                return Response(
                    content=json.dumps(
                        {
                            "detail": f"No batch source for reference view '{ref_fv_name}'"
                        }
                    ),
                    status_code=status.HTTP_400_BAD_REQUEST,
                    media_type="application/json",
                )

            ref_join_keys = (
                ref_fv.join_keys
                if hasattr(ref_fv, "join_keys")
                else (ref_fv.entities if ref_fv.entities else [])
            )
            ref_feature_names = [f.name for f in ref_fv.features]

            try:
                ref_job = provider.offline_store.pull_all_from_table_or_query(
                    config=store.config,
                    data_source=ref_batch_source,
                    join_key_columns=ref_join_keys,
                    feature_name_columns=ref_feature_names[:1],
                    timestamp_field=ref_batch_source.timestamp_field,
                    created_timestamp_column=getattr(
                        ref_batch_source, "created_timestamp_column", None
                    ),
                )
                ref_df = ref_job.to_df()
            except Exception:
                return Response(
                    content=json.dumps(
                        {
                            "detail": f"Could not read reference feature view '{ref_fv_name}'. Ensure data exists."
                        }
                    ),
                    status_code=status.HTTP_400_BAD_REQUEST,
                    media_type="application/json",
                )

            # Get labeled entities from the label view
            label_batch_source = getattr(fv, "batch_source", None)
            label_join_keys = (
                fv.join_keys
                if hasattr(fv, "join_keys")
                else (fv.entities if fv.entities else [])
            )
            label_feature_names = [f.name for f in fv.features]

            labeled_keys: set = set()
            if label_batch_source:
                try:
                    label_job = provider.offline_store.pull_all_from_table_or_query(
                        config=store.config,
                        data_source=label_batch_source,
                        join_key_columns=label_join_keys,
                        feature_name_columns=label_feature_names,
                        timestamp_field=label_batch_source.timestamp_field,
                        created_timestamp_column=getattr(
                            label_batch_source,
                            "created_timestamp_column",
                            None,
                        ),
                    )
                    label_df = label_job.to_df()
                    if not label_df.empty and label_join_keys:
                        labeled_keys = set(
                            label_df[label_join_keys[0]].dropna().astype(str)
                        )
                except Exception:
                    labeled_keys = set()

            # Find unlabeled entities (in ref but not in labels)
            common_key = label_join_keys[0] if label_join_keys else ref_join_keys[0]
            unlabeled_entities: List[Dict[str, Any]] = []

            if common_key in ref_df.columns:
                seen_keys: set = set()
                for _, row in ref_df.iterrows():
                    key_val = (
                        str(row[common_key]) if pd.notna(row[common_key]) else None
                    )
                    if (
                        key_val
                        and key_val not in labeled_keys
                        and key_val not in seen_keys
                    ):
                        seen_keys.add(key_val)
                        entity = {common_key: row[common_key]}
                        unlabeled_entities.append(entity)
                        if len(unlabeled_entities) >= (request.limit or 50):
                            break

            return {
                "unlabeled_entities": unlabeled_entities,
                "total_labeled": len(labeled_keys),
                "total_unlabeled": len(unlabeled_entities),
                "entity_names": label_join_keys,
                "feature_names": label_feature_names,
                "label_view": fv_name,
                "reference_feature_view": ref_fv_name,
            }
        except Exception:
            logger.exception("Active learning candidates failed")
            return _safe_error_response("Active learning candidates")

    # --- Production Label Management Endpoints ---

    class BatchPushRequest(BaseModel):
        push_source_name: str
        data: List[Dict[str, Any]]
        to: Optional[str] = "online"

    @rest_app.post("/batch-push")
    def batch_push(request: BatchPushRequest):
        """Batch push labels from CSV/parquet upload or external systems (Argilla, Label Studio)."""
        try:
            df = pd.DataFrame(request.data)
            to = request.to or "online"
            if to == "online_and_offline":
                store.push(
                    request.push_source_name,
                    df,
                    to=feast.data_source.PushMode.ONLINE_AND_OFFLINE,
                )
            elif to == "offline":
                store.push(
                    request.push_source_name, df, to=feast.data_source.PushMode.OFFLINE
                )
            else:
                store.push(
                    request.push_source_name, df, to=feast.data_source.PushMode.ONLINE
                )
            return {"status": "ok", "rows_pushed": len(df)}
        except Exception:
            logger.exception("Batch push failed")
            return _safe_error_response("Batch push")

    class WebhookPushRequest(BaseModel):
        push_source_name: str
        records: List[Dict[str, Any]]
        api_key: Optional[str] = None

    @rest_app.post("/webhook/label-ingest")
    def webhook_label_ingest(request: WebhookPushRequest):
        """Webhook endpoint for external annotation tools (Argilla, Label Studio) to push labels."""
        try:
            from datetime import datetime, timezone

            for record in request.records:
                if "event_timestamp" not in record:
                    record["event_timestamp"] = datetime.now(timezone.utc).isoformat()

            df = pd.DataFrame(request.records)
            store.push(
                request.push_source_name,
                df,
                to=feast.data_source.PushMode.ONLINE_AND_OFFLINE,
            )
            return {
                "status": "ok",
                "records_ingested": len(request.records),
                "push_source": request.push_source_name,
            }
        except Exception:
            logger.exception("Webhook label ingest failed")
            return _safe_error_response("Webhook label ingest")

    class TrainingExportRequest(BaseModel):
        feature_service: str
        entity_df: Dict[str, List]
        start_date: Optional[str] = None
        end_date: Optional[str] = None

    @rest_app.post("/training-dataset/export")
    def export_training_dataset(request: TrainingExportRequest):
        """Generate a training dataset using get_historical_features and return as JSON (downloadable)."""
        try:
            from datetime import datetime, timezone

            entity_df = pd.DataFrame(request.entity_df)

            if "event_timestamp" not in entity_df.columns:
                if request.end_date:
                    ts = pd.Timestamp(request.end_date, tz="UTC")
                else:
                    ts = pd.Timestamp(datetime.now(timezone.utc))
                entity_df["event_timestamp"] = ts

            fs = store.get_feature_service(request.feature_service)
            training_df = store.get_historical_features(
                entity_df=entity_df,
                features=fs,
            ).to_df()

            result = training_df.to_dict(orient="records")
            for row in result:
                for k, v in row.items():
                    if pd.isna(v):
                        row[k] = None
                    elif hasattr(v, "isoformat"):
                        row[k] = v.isoformat()

            return {
                "data": result,
                "columns": list(training_df.columns),
                "row_count": len(training_df),
                "feature_service": request.feature_service,
            }
        except Exception:
            logger.exception("Training dataset export failed")
            return _safe_error_response("Training dataset export")

    @rest_app.get("/webhook/config/{label_view_name}")
    def webhook_config(label_view_name: str):
        """Return webhook configuration info for integrating external annotation tools."""
        try:
            fv = store.registry.get_label_view(label_view_name, store.project)
            push_source_name = fv.source.name if fv.source else None
            feature_names = [f.name for f in fv.features]
            entity_names = [ec.name for ec in fv.entity_columns]
            labeler_field = getattr(fv, "labeler_field", None)

            return {
                "label_view": label_view_name,
                "push_source_name": push_source_name,
                "webhook_url": "/api/v1/webhook/label-ingest",
                "batch_url": "/api/v1/batch-push",
                "required_fields": entity_names
                + feature_names
                + ([labeler_field] if labeler_field else []),
                "entity_fields": entity_names,
                "label_fields": feature_names,
                "labeler_field": labeler_field,
                "payload_example": {
                    "push_source_name": push_source_name,
                    "records": [
                        {
                            **{e: f"<{e}_value>" for e in entity_names},
                            **{f: f"<{f}_value>" for f in feature_names},
                            **(
                                {"event_timestamp": "2026-01-01T00:00:00Z"}
                                if True
                                else {}
                            ),
                            **(
                                {labeler_field: "<labeler_id>"} if labeler_field else {}
                            ),
                        }
                    ],
                },
            }
        except Exception:
            logger.exception("Webhook config lookup failed")
            return _safe_error_response("Webhook config", status.HTTP_404_NOT_FOUND)

    @rest_app.get("/annotation-config/{label_view_name}")
    def annotation_config(label_view_name: str):
        """Return annotation profile and field role config parsed from LabelView tags.

        The UI uses this to select the right annotation method and map
        user interactions to schema fields.
        """
        try:
            fv = store.registry.get_label_view(label_view_name, store.project)

            tags = dict(getattr(fv, "tags", {}))
            profile = tags.get("feast.io/labeling-method", "table")

            field_roles: Dict[str, str] = {}
            label_values: Dict[str, List] = {}
            label_widgets: Dict[str, str] = {}

            for key, value in tags.items():
                if key.startswith("feast.io/field-role:"):
                    field_name = key[len("feast.io/field-role:") :]
                    field_roles[field_name] = value
                elif key.startswith("feast.io/label-values:"):
                    field_name = key[len("feast.io/label-values:") :]
                    label_values[field_name] = [v.strip() for v in value.split(",")]
                elif key.startswith("feast.io/label-widget:"):
                    field_name = key[len("feast.io/label-widget:") :]
                    label_widgets[field_name] = value

            entity_names = fv.entities if fv.entities else []
            feature_names = [f.name for f in fv.features]
            labeler_field = getattr(fv, "labeler_field", "labeler")
            push_source_name = fv.source.name if fv.source else None

            return {
                "label_view": label_view_name,
                "profile": profile,
                "field_roles": field_roles,
                "label_values": label_values,
                "label_widgets": label_widgets,
                "entities": entity_names,
                "features": feature_names,
                "labeler_field": labeler_field,
                "push_source_name": push_source_name,
            }
        except Exception:
            logger.exception("Annotation config lookup failed")
            return _safe_error_response("Annotation config", status.HTTP_404_NOT_FOUND)

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

            safe_project = project_name.replace("\\", "\\\\").replace("'", "\\'")
            filter_str = (
                f"tags.`feast.project` = '{safe_project}' "
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
            return {"feature_usage": {}, "mlflow_enabled": False}

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
                result = {"feature_usage": {}, "mlflow_enabled": True}
                _feature_usage_cache["data"] = result
                _feature_usage_cache["timestamp"] = now
                return result

            safe_project = project_name.replace("\\", "\\\\").replace("'", "\\'")
            filter_str = (
                f"tags.`feast.project` = '{safe_project}' "
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

            result = {"feature_usage": usage, "mlflow_enabled": True}
            _feature_usage_cache["data"] = result
            _feature_usage_cache["timestamp"] = now
            return result
        except ImportError:
            return {
                "feature_usage": {},
                "mlflow_enabled": False,
                "error": "mlflow is not installed",
            }
        except Exception as e:
            logger.debug("Failed to fetch feature usage: %s", e)
            return {
                "feature_usage": {},
                "mlflow_enabled": True,
                "error": "Failed to fetch usage data",
            }

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
            logger.debug("Failed to fetch MLflow feature-model mapping: %s", e)
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
