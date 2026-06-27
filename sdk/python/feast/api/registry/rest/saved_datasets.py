import logging
from typing import Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel

from feast.api.registry.rest.codegen_utils import render_saved_dataset_code
from feast.api.registry.rest.rest_utils import (
    aggregate_across_projects,
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
    parse_tags,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDataset as SavedDatasetProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetMeta,
    SavedDatasetSpec,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


class RegisterDatasetRequest(BaseModel):
    name: str
    project: str
    features: List[str] = []
    join_keys: List[str] = []
    storage_path: Optional[str] = None
    storage_type: str = "file"
    tags: Dict[str, str] = {}
    full_feature_names: bool = False
    feature_service_name: Optional[str] = None
    allow_override: bool = False


class CreateDatasetRequest(BaseModel):
    name: str
    project: str
    feature_service_name: Optional[str] = None
    features: Optional[List[str]] = None
    entity_source_type: str = "inline"
    entity_source_path: Optional[str] = None
    # Inline entity definition
    entity_keys: Optional[List[str]] = None
    entity_values: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    extra_columns: Optional[str] = None
    # Storage
    storage_type: str = "file"
    storage_path: str = ""
    tags: Dict[str, str] = {}
    allow_overwrite: bool = False


def get_saved_dataset_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/saved_datasets/all")
    def list_saved_datasets_all(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each saved dataset"
        ),
    ):
        return aggregate_across_projects(
            grpc_handler=grpc_handler,
            list_method=grpc_handler.ListSavedDatasets,
            request_cls=RegistryServer_pb2.ListSavedDatasetsRequest,
            response_key="savedDatasets",
            object_type="savedDataset",
            allow_cache=allow_cache,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            include_relationships=include_relationships,
        )

    @router.get("/saved_datasets/data/{name}")
    def get_dataset_data(
        name: str,
        project: str = Query(...),
        limit: int = Query(10, ge=1, le=100),
    ):
        import json

        req = RegistryServer_pb2.GetDatasetDataRequest(
            name=name, project=project, limit=limit
        )
        try:
            result = grpc_call(grpc_handler.GetDatasetData, req)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to read dataset data: {str(e)}",
            )

        columns = result.get("columns", [])
        raw_rows = result.get("rows", [])
        rows = []
        for raw_row in raw_rows:
            values = raw_row.get("values", [])
            row_dict = {}
            for i, col in enumerate(columns):
                if i < len(values):
                    try:
                        row_dict[col] = json.loads(values[i])
                    except (json.JSONDecodeError, TypeError):
                        row_dict[col] = values[i]
                else:
                    row_dict[col] = ""
            rows.append(row_dict)

        return {
            "columns": columns,
            "rows": rows,
            "total_rows": result.get("totalRows", 0),
            "sample_size": result.get("sampleSize", 0),
        }

    @router.get("/saved_datasets/jobs/{job_id}")
    def get_dataset_job_status(job_id: str):
        req = RegistryServer_pb2.GetDatasetJobStatusRequest(job_id=job_id)
        result = grpc_call(grpc_handler.GetDatasetJobStatus, req)
        return {
            "job_id": result.get("jobId", ""),
            "dataset_name": result.get("datasetName", ""),
            "project": result.get("project", ""),
            "status": result.get("status", ""),
            "created_at": result.get("createdAt", ""),
            "completed_at": result.get("completedAt", ""),
            "error": result.get("error", ""),
        }

    @router.get("/saved_datasets/jobs")
    def list_dataset_jobs(
        project: str = Query(""),
        status: Optional[str] = Query(None),
    ):
        req = RegistryServer_pb2.ListDatasetJobsRequest(
            project=project or "",
            status_filter=status or "",
        )
        result = grpc_call(grpc_handler.ListDatasetJobs, req)
        jobs = result.get("jobs", [])
        return {
            "jobs": [
                {
                    "job_id": j.get("jobId", ""),
                    "dataset_name": j.get("datasetName", ""),
                    "project": j.get("project", ""),
                    "status": j.get("status", ""),
                    "created_at": j.get("createdAt", ""),
                    "completed_at": j.get("completedAt", ""),
                    "error": j.get("error", ""),
                }
                for j in jobs
            ]
        }

    @router.get("/saved_datasets/{name}")
    def get_saved_dataset(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this saved dataset"
        ),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetSavedDatasetRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        saved_dataset = grpc_call(grpc_handler.GetSavedDataset, req)

        result = saved_dataset

        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "savedDataset", name, project, allow_cache
            )
            result["relationships"] = relationships

        if result:
            spec = result.get("spec", result)
            features = spec.get("features", [])
            features_exprs = []
            for f in features:
                if isinstance(f, dict) and f.get("name"):
                    view_name = f["name"]
                    feature_names = f.get("features", [])
                    if feature_names:
                        features_exprs.append(
                            f"{view_name}[[{', '.join([repr(fn) for fn in feature_names])}]]"
                        )
                    else:
                        features_exprs.append(view_name)
                else:
                    features_exprs.append(str(f))
            features_str = ", ".join(features_exprs)
            context = dict(
                name=spec.get("name", name),
                features=features_str,
                tags=spec.get("tags", {}),
            )
            result["featureDefinition"] = render_saved_dataset_code(context)

        return result

    @router.get("/saved_datasets")
    def list_saved_datasets(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
        include_relationships: bool = Query(
            False, description="Include relationships for each saved dataset"
        ),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListSavedDatasetsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListSavedDatasets, req)
        saved_datasets = response.get("savedDatasets", [])

        result = {
            "savedDatasets": saved_datasets,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, saved_datasets, "savedDataset", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.post("/saved_datasets")
    def register_saved_dataset(payload: RegisterDatasetRequest = Body(...)):
        if not payload.name or not payload.name.strip():
            raise HTTPException(status_code=400, detail="Dataset name is required.")
        if not payload.project or not payload.project.strip():
            raise HTTPException(status_code=400, detail="Project is required.")
        if not payload.storage_path or not payload.storage_path.strip():
            raise HTTPException(
                status_code=400,
                detail="Storage path is required. Provide a file path or table reference.",
            )

        if not payload.allow_override:
            try:
                req = RegistryServer_pb2.GetSavedDatasetRequest(
                    name=payload.name.strip(),
                    project=payload.project.strip(),
                    allow_cache=False,
                )
                grpc_call(grpc_handler.GetSavedDataset, req)
                raise HTTPException(
                    status_code=409,
                    detail=f'A dataset named "{payload.name.strip()}" already exists in project "{payload.project.strip()}". Use edit to update it.',
                )
            except HTTPException:
                raise
            except Exception:
                pass

        storage_proto = SavedDatasetStorageProto()
        path = payload.storage_path.strip()
        if payload.storage_type == "bigquery":
            storage_proto.bigquery_storage.CopyFrom(
                DataSourceProto.BigQueryOptions(table=path)
            )
        elif payload.storage_type == "redshift":
            storage_proto.redshift_storage.CopyFrom(
                DataSourceProto.RedshiftOptions(table=path)
            )
        elif payload.storage_type == "snowflake":
            storage_proto.snowflake_storage.CopyFrom(
                DataSourceProto.SnowflakeOptions(table=path)
            )
        elif payload.storage_type == "trino":
            storage_proto.trino_storage.CopyFrom(
                DataSourceProto.TrinoOptions(table=path)
            )
        elif payload.storage_type == "spark":
            storage_proto.spark_storage.CopyFrom(
                DataSourceProto.SparkOptions(path=path)
            )
        elif payload.storage_type == "athena":
            storage_proto.athena_storage.CopyFrom(
                DataSourceProto.AthenaOptions(table=path)
            )
        elif payload.storage_type == "custom":
            storage_proto.custom_storage.CopyFrom(
                DataSourceProto.CustomSourceOptions(configuration=path.encode("utf-8"))
            )
        else:
            storage_proto.file_storage.CopyFrom(DataSourceProto.FileOptions(uri=path))

        # Auto-populate features and join_keys from feature service if not provided
        features = payload.features
        join_keys = payload.join_keys
        if payload.feature_service_name and (not features or not join_keys):
            try:
                derived_features, derived_keys = (
                    grpc_handler.resolve_feature_service_metadata(
                        payload.feature_service_name.strip(),
                        payload.project.strip(),
                    )
                )
                if not features and derived_features:
                    features = derived_features
                if not join_keys and derived_keys:
                    join_keys = derived_keys
            except Exception:
                pass

        spec = SavedDatasetSpec(
            name=payload.name,
            project=payload.project,
            features=features or [],
            join_keys=join_keys or [],
            full_feature_names=payload.full_feature_names,
            storage=storage_proto,
            tags=payload.tags,
        )
        if payload.feature_service_name:
            spec.feature_service_name = payload.feature_service_name

        saved_dataset_proto = SavedDatasetProto(
            spec=spec,
            meta=SavedDatasetMeta(),
        )

        apply_req = RegistryServer_pb2.ApplySavedDatasetRequest(
            saved_dataset=saved_dataset_proto,
            project=payload.project,
            commit=True,
        )
        grpc_call(grpc_handler.ApplySavedDataset, apply_req)
        return {"status": "ok", "name": payload.name, "project": payload.project}

    @router.delete("/saved_datasets/{name}")
    def delete_saved_dataset(
        name: str,
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.DeleteSavedDatasetRequest(
            name=name,
            project=project,
            commit=True,
        )
        grpc_call(grpc_handler.DeleteSavedDataset, req)
        return {"status": "ok", "name": name, "project": project}

    @router.post("/saved_datasets/create")
    def create_dataset(payload: CreateDatasetRequest = Body(...)):
        if not payload.name or not payload.name.strip():
            raise HTTPException(status_code=400, detail="Dataset name is required.")
        if not payload.project or not payload.project.strip():
            raise HTTPException(status_code=400, detail="Project is required.")
        if not payload.storage_path or not payload.storage_path.strip():
            raise HTTPException(
                status_code=400, detail="Storage path (output destination) is required."
            )
        if not payload.feature_service_name and not payload.features:
            raise HTTPException(
                status_code=400,
                detail="Either feature_service_name or features list is required.",
            )

        if payload.entity_source_type == "inline":
            if not payload.entity_keys:
                raise HTTPException(
                    status_code=400,
                    detail="entity_keys is required when entity_source_type is 'inline'.",
                )
            if not payload.entity_values or not payload.entity_values.strip():
                raise HTTPException(
                    status_code=400,
                    detail="entity_values is required when entity_source_type is 'inline'.",
                )
        elif payload.entity_source_type == "reference":
            if not payload.entity_source_path:
                raise HTTPException(
                    status_code=400,
                    detail="entity_source_path is required when entity_source_type is 'reference'.",
                )

        req = RegistryServer_pb2.CreateDatasetFromRetrievalRequest(
            name=payload.name.strip(),
            project=payload.project.strip(),
            feature_service_name=payload.feature_service_name or "",
            features=payload.features or [],
            entity_source_type=payload.entity_source_type,
            entity_source_path=payload.entity_source_path or "",
            entity_keys=payload.entity_keys or [],
            entity_values=payload.entity_values or "",
            start_date=payload.start_date or "",
            end_date=payload.end_date or "",
            extra_columns=payload.extra_columns or "",
            storage_type=payload.storage_type,
            storage_path=payload.storage_path.strip(),
            tags=payload.tags or {},
            allow_overwrite=payload.allow_overwrite,
        )
        try:
            result = grpc_call(grpc_handler.CreateDatasetFromRetrieval, req)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create dataset: {str(e)}",
            )

        return {
            "job_id": result.get("jobId", ""),
            "status": result.get("status", ""),
        }

    return router
