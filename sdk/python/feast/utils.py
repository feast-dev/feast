import copy
import itertools
import os
import typing
import warnings
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import pandas as pd
import pyarrow
from dateutil.tz import tzlocal
from google.protobuf.timestamp_pb2 import Timestamp

from feast.constants import FEAST_FS_YAML_FILE_PATH_ENV_NAME
from feast.entity import Entity
from feast.errors import (
    FeatureNameCollisionError,
    FeatureViewNotFoundException,
    RequestDataNotFoundInEntityRowsException,
)
from feast.field import Field
from feast.infra.compute_engines.backends.pandas_backend import PandasBackend
from feast.infra.key_encoding_utils import deserialize_entity_key
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList as FloatListProto
from feast.protos.feast.types.Value_pb2 import RepeatedValue as RepeatedValueProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.type_map import python_values_to_proto_values
from feast.types import ComplexFeastType, PrimitiveFeastType, from_feast_to_pyarrow_type
from feast.value_type import ValueType
from feast.version import get_version

if typing.TYPE_CHECKING:
    from feast.base_feature_view import BaseFeatureView
    from feast.feature_service import FeatureService
    from feast.feature_view import FeatureView
    from feast.infra.registry.base_registry import BaseRegistry
    from feast.on_demand_feature_view import OnDemandFeatureView

APPLICATION_NAME = "feast-dev/feast"
USER_AGENT = "{}/{}".format(APPLICATION_NAME, get_version())


def get_user_agent():
    return USER_AGENT


def make_tzaware(t: datetime) -> datetime:
    """We assume tz-naive datetimes are UTC"""
    if t.tzinfo is None:
        return t.replace(tzinfo=timezone.utc)
    else:
        return t


def make_df_tzaware(t: pd.DataFrame) -> pd.DataFrame:
    """Make all datetime type columns tzaware; leave everything else intact."""
    df = t.copy()  # don't modify incoming dataframe inplace
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = pd.to_datetime(df[column], utc=True)
    return df


def to_naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(timezone.utc).replace(tzinfo=None)


def maybe_local_tz(t: datetime) -> datetime:
    if t.tzinfo is None:
        return t.replace(tzinfo=tzlocal())
    else:
        return t


def get_default_yaml_file_path(repo_path: Path) -> Path:
    if FEAST_FS_YAML_FILE_PATH_ENV_NAME in os.environ:
        yaml_path = os.environ[FEAST_FS_YAML_FILE_PATH_ENV_NAME]
        return Path(yaml_path)
    else:
        return repo_path / "feature_store.yaml"


def _get_requested_feature_views_to_features_dict(
    feature_refs: List[str],
    feature_views: List["FeatureView"],
    on_demand_feature_views: List["OnDemandFeatureView"],
) -> Tuple[Dict["FeatureView", List[str]], Dict["OnDemandFeatureView", List[str]]]:
    """Create a dict of FeatureView -> List[Feature] for all requested features.
    Set full_feature_names to True to have feature names prefixed by their feature view name.
    """

    feature_views_to_feature_map: Dict["FeatureView", List[str]] = defaultdict(list)
    on_demand_feature_views_to_feature_map: Dict["OnDemandFeatureView", List[str]] = (
        defaultdict(list)
    )

    for ref in feature_refs:
        ref_parts = ref.split(":")
        feature_view_from_ref = ref_parts[0]
        feature_from_ref = ref_parts[1]

        found = False
        for fv in feature_views:
            if fv.projection.name_to_use() == feature_view_from_ref:
                found = True
                feature_views_to_feature_map[fv].append(feature_from_ref)
        for odfv in on_demand_feature_views:
            if odfv.projection.name_to_use() == feature_view_from_ref:
                found = True
                on_demand_feature_views_to_feature_map[odfv].append(feature_from_ref)

        if not found:
            raise ValueError(f"Could not find feature view from reference {ref}")

    return feature_views_to_feature_map, on_demand_feature_views_to_feature_map


def _get_column_names(
    feature_view: "FeatureView", entities: List[Entity]
) -> Tuple[List[str], List[str], str, Optional[str]]:
    """
    If a field mapping exists, run it in reverse on the join keys,
    feature names, event timestamp column, and created timestamp column
    to get the names of the relevant columns in the offline feature store table.

    Returns:
        Tuple containing the list of reverse-mapped join_keys,
        reverse-mapped feature names, reverse-mapped event timestamp column,
        and reverse-mapped created timestamp column that will be passed into
        the query to the offline store.
    """
    # if we have mapped fields, use the original field names in the call to the offline store
    timestamp_field = feature_view.batch_source.timestamp_field
    feature_names = [feature.name for feature in feature_view.features]
    created_timestamp_column = feature_view.batch_source.created_timestamp_column

    from feast.feature_view import DUMMY_ENTITY_ID

    join_keys = [
        entity.join_key for entity in entities if entity.join_key != DUMMY_ENTITY_ID
    ]
    if feature_view.batch_source.field_mapping is not None:
        reverse_field_mapping = {
            v: k for k, v in feature_view.batch_source.field_mapping.items()
        }
        timestamp_field = (
            reverse_field_mapping[timestamp_field]
            if timestamp_field in reverse_field_mapping.keys()
            else timestamp_field
        )
        created_timestamp_column = (
            reverse_field_mapping[created_timestamp_column]
            if created_timestamp_column
            and created_timestamp_column in reverse_field_mapping.keys()
            else created_timestamp_column
        )
        join_keys = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in join_keys
        ]
        feature_names = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in feature_names
        ]

    # We need to exclude join keys and timestamp columns from the list of features, after they are mapped to
    # their final column names via the `field_mapping` field of the source.
    feature_names = [
        name
        for name in feature_names
        if name not in join_keys
        and name != timestamp_field
        and name != created_timestamp_column
    ]
    return (
        join_keys,
        feature_names,
        timestamp_field,
        created_timestamp_column,
    )


def _run_pyarrow_field_mapping(
    table: pyarrow.Table,
    field_mapping: Dict[str, str],
) -> pyarrow.Table:
    # run field mapping in the forward direction
    cols = table.column_names
    mapped_cols = [
        field_mapping[col] if col in field_mapping.keys() else col for col in cols
    ]
    table = table.rename_columns(mapped_cols)
    return table


def _get_fields_with_aliases(
    fields: List[str],
    field_mappings: Dict[str, str],
) -> Tuple[List[str], List[str]]:
    """
    Get a list of fields with aliases based on the field mappings.
    """
    for field in fields:
        if "." in field and field not in field_mappings:
            raise ValueError(
                f"Feature {field} contains a '.' character, which is not allowed in field names. Use field mappings to rename fields."
            )
    fields_with_aliases = [
        f"{field} AS {field_mappings[field]}" if field in field_mappings else field
        for field in fields
    ]
    aliases = [
        field_mappings[field] if field in field_mappings else field for field in fields
    ]
    return (fields_with_aliases, aliases)


def _coerce_datetime(ts):
    """
    Depending on underlying time resolution, arrow to_pydict() sometimes returns pd
    timestamp type (for nanosecond resolution), and sometimes you get standard python datetime
    (for microsecond resolution).
    While pd timestamp class is a subclass of python datetime, it doesn't always behave the
    same way. We convert it to normal datetime so that consumers downstream don't have to deal
    with these quirks.
    """
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime()
    else:
        return ts


def _convert_arrow_to_proto(
    table: Union[pyarrow.Table, pyarrow.RecordBatch],
    feature_view: Union["FeatureView", "BaseFeatureView", "OnDemandFeatureView"],
    join_keys: Dict[str, ValueType],
) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
    # This is a workaround for isinstance(feature_view, OnDemandFeatureView), which triggers a circular import
    # Check for source_request_sources or source_feature_view_projections attributes to identify ODFVs
    if (
        getattr(feature_view, "source_request_sources", None) is not None
        or getattr(feature_view, "source_feature_view_projections", None) is not None
    ):
        return _convert_arrow_odfv_to_proto(table, feature_view, join_keys)  # type: ignore[arg-type]
    else:
        return _convert_arrow_fv_to_proto(table, feature_view, join_keys)  # type: ignore[arg-type]


def _convert_arrow_fv_to_proto(
    table: Union[pyarrow.Table, pyarrow.RecordBatch],
    feature_view: "FeatureView",
    join_keys: Dict[str, ValueType],
) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
    # Avoid ChunkedArrays which guarantees `zero_copy_only` available.
    if isinstance(table, pyarrow.Table):
        table = table.to_batches()[0]

    # TODO: This will break if the feature view has aggregations or transformations
    columns = [
        (field.name, field.dtype.to_value_type()) for field in feature_view.features
    ] + list(join_keys.items())

    proto_values_by_column = {
        column: python_values_to_proto_values(
            table.column(column).to_numpy(zero_copy_only=False), value_type
        )
        for column, value_type in columns
    }

    entity_keys = [
        EntityKeyProto(
            join_keys=join_keys,
            entity_values=[proto_values_by_column[k][idx] for k in join_keys],
        )
        for idx in range(table.num_rows)
    ]

    # Serialize the features per row
    feature_dict = {
        feature.name: proto_values_by_column[feature.name]
        for feature in feature_view.features
    }
    features = [dict(zip(feature_dict, vars)) for vars in zip(*feature_dict.values())]

    # Convert event_timestamps
    event_timestamps = [
        _coerce_datetime(val)
        for val in pd.to_datetime(
            table.column(feature_view.batch_source.timestamp_field).to_numpy(
                zero_copy_only=False
            )
        )
    ]

    # Convert created_timestamps if they exist
    if feature_view.batch_source.created_timestamp_column:
        created_timestamps = [
            _coerce_datetime(val)
            for val in pd.to_datetime(
                table.column(
                    feature_view.batch_source.created_timestamp_column
                ).to_numpy(zero_copy_only=False)
            )
        ]
    else:
        created_timestamps = [None] * table.num_rows

    return list(zip(entity_keys, features, event_timestamps, created_timestamps))


def _convert_arrow_odfv_to_proto(
    table: Union[pyarrow.Table, pyarrow.RecordBatch],
    feature_view: "OnDemandFeatureView",
    join_keys: Dict[str, ValueType],
) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
    # Avoid ChunkedArrays which guarantees `zero_copy_only` available.
    if isinstance(table, pyarrow.Table):
        table = table.to_batches()[0]

    columns = [
        (field.name, field.dtype.to_value_type()) for field in feature_view.features
    ] + list(join_keys.items())

    proto_values_by_column = {
        column: python_values_to_proto_values(
            table.column(column).to_numpy(zero_copy_only=False), value_type
        )
        for column, value_type in columns
        if column in table.column_names
    }

    # Ensure join keys are included in proto_values_by_column, but check if they exist first
    for join_key, value_type in join_keys.items():
        if join_key not in proto_values_by_column:
            # Check if the join key exists in the table before trying to access it
            if join_key in table.column_names:
                proto_values_by_column[join_key] = python_values_to_proto_values(
                    table.column(join_key).to_numpy(zero_copy_only=False), value_type
                )
            else:
                # Create null/default values if the join key isn't in the table
                null_column = [None] * table.num_rows
                proto_values_by_column[join_key] = python_values_to_proto_values(
                    null_column, value_type
                )

    # Adding On Demand Features
    for feature in feature_view.features:
        if (
            feature.name in [c[0] for c in columns]
            and feature.name not in proto_values_by_column
        ):
            # initializing the column as null
            null_column = pyarrow.array(
                [None] * table.num_rows,
                type=from_feast_to_pyarrow_type(feature.dtype),
            )
            updated_table = pyarrow.RecordBatch.from_arrays(
                table.columns + [null_column],
                schema=table.schema.append(
                    pyarrow.field(feature.name, null_column.type)  # type: ignore[attr-defined]
                ),
            )
            proto_values_by_column[feature.name] = python_values_to_proto_values(
                updated_table.column(feature.name).to_numpy(zero_copy_only=False),
                feature.dtype.to_value_type(),
            )

    entity_keys = [
        EntityKeyProto(
            join_keys=join_keys,
            entity_values=[
                proto_values_by_column[k][idx]
                for k in join_keys
                if k in proto_values_by_column
            ],
        )
        for idx in range(table.num_rows)
    ]

    # Serialize the features per row
    feature_dict = {
        feature.name: proto_values_by_column[feature.name]
        for feature in feature_view.features
        if feature.name in proto_values_by_column
    }
    if feature_view.write_to_online_store:
        table_columns = [col.name for col in table.schema]
        for feature in feature_view.schema:
            if feature.name not in feature_dict and feature.name in table_columns:
                feature_dict[feature.name] = proto_values_by_column[feature.name]

    features = [dict(zip(feature_dict, vars)) for vars in zip(*feature_dict.values())]

    # We need to artificially add event_timestamps and created_timestamps
    event_timestamps = []
    timestamp_values = pd.to_datetime([_utc_now() for i in range(table.num_rows)])

    for val in timestamp_values:
        event_timestamps.append(_coerce_datetime(val))

    # setting them equivalent
    created_timestamps = event_timestamps

    return list(zip(entity_keys, features, event_timestamps, created_timestamps))


def _validate_entity_values(join_key_values: Dict[str, List[ValueProto]]):
    set_of_row_lengths = {len(v) for v in join_key_values.values()}
    if len(set_of_row_lengths) > 1:
        raise ValueError("All entity rows must have the same columns.")
    return set_of_row_lengths.pop()


def _validate_feature_refs(feature_refs: List[str], full_feature_names: bool = False):
    """
    Validates that there are no collisions among the feature references.

    Args:
        feature_refs: List of feature references to validate. Feature references must have format
            "feature_view:feature", e.g. "customer_fv:daily_transactions".
        full_feature_names: If True, the full feature references are compared for collisions; if False,
            only the feature names are compared.

    Raises:
        FeatureNameCollisionError: There is a collision among the feature references.
    """
    collided_feature_refs = []

    if full_feature_names:
        collided_feature_refs = [
            ref for ref, occurrences in Counter(feature_refs).items() if occurrences > 1
        ]
    else:
        feature_names = [ref.split(":")[1] for ref in feature_refs]
        collided_feature_names = [
            ref
            for ref, occurrences in Counter(feature_names).items()
            if occurrences > 1
        ]

        for feature_name in collided_feature_names:
            collided_feature_refs.extend(
                [ref for ref in feature_refs if ref.endswith(":" + feature_name)]
            )

    if len(collided_feature_refs) > 0:
        raise FeatureNameCollisionError(collided_feature_refs, full_feature_names)


def _group_feature_refs(
    features: List[str],
    all_feature_views: List["FeatureView"],
    all_on_demand_feature_views: List["OnDemandFeatureView"],
) -> Tuple[
    List[Tuple[Union["FeatureView", "OnDemandFeatureView"], List[str]]],
    List[Tuple["OnDemandFeatureView", List[str]]],
]:
    """Get list of feature views and corresponding feature names based on feature references"""

    # view name to view proto
    view_index: Dict[str, Union["FeatureView", "OnDemandFeatureView"]] = {
        view.projection.name_to_use(): view for view in all_feature_views
    }

    # on demand view to on demand view proto
    on_demand_view_index: Dict[str, "OnDemandFeatureView"] = {}
    for view in all_on_demand_feature_views:
        if view.projection and not view.write_to_online_store:
            on_demand_view_index[view.projection.name_to_use()] = view
        elif view.projection and view.write_to_online_store:
            # we insert the ODFV view to FVs for ones that are written to the online store
            view_index[view.projection.name_to_use()] = view

    # view name to feature names
    views_features = defaultdict(set)

    # on demand view name to feature names
    on_demand_view_features = defaultdict(set)

    for ref in features:
        view_name, feat_name = ref.split(":")
        if view_name in view_index:
            if hasattr(view_index[view_name], "write_to_online_store"):
                tmp_feat_name = [
                    f for f in view_index[view_name].schema if f.name == feat_name
                ]
                if len(tmp_feat_name) > 0:
                    feat_name = tmp_feat_name[0].name
            else:
                view_index[view_name].projection.get_feature(
                    feat_name
                )  # For validation
            views_features[view_name].add(feat_name)
        elif view_name in on_demand_view_index:
            on_demand_view_index[view_name].projection.get_feature(
                feat_name
            )  # For validation
            on_demand_view_features[view_name].add(feat_name)
            # Let's also add in any FV Feature dependencies here.
            for input_fv_projection in on_demand_view_index[
                view_name
            ].source_feature_view_projections.values():
                for input_feat in input_fv_projection.features:
                    views_features[input_fv_projection.name].add(input_feat.name)
        else:
            raise FeatureViewNotFoundException(view_name)

    fvs_result: List[Tuple[Union["FeatureView", "OnDemandFeatureView"], List[str]]] = []
    odfvs_result: List[Tuple["OnDemandFeatureView", List[str]]] = []

    for view_name, feature_names in views_features.items():
        fvs_result.append((view_index[view_name], list(feature_names)))
    for view_name, feature_names in on_demand_view_features.items():
        odfvs_result.append((on_demand_view_index[view_name], list(feature_names)))
    return fvs_result, odfvs_result


def construct_response_feature_vector(
    values_vector: Iterable[Any],
    statuses_vector: Iterable[Any],
    timestamp_vector: Iterable[Any],
    mapping_indexes: Iterable[List[int]],
    output_len: int,
) -> GetOnlineFeaturesResponse.FeatureVector:
    values_output: Iterable[Any] = [None] * output_len
    statuses_output: Iterable[Any] = [None] * output_len
    timestamp_output: Iterable[Any] = [None] * output_len

    for i, destinations in enumerate(mapping_indexes):
        for idx in destinations:
            values_output[idx] = values_vector[i]  # type: ignore[index]
            statuses_output[idx] = statuses_vector[i]  # type: ignore[index]
            timestamp_output[idx] = timestamp_vector[i]  # type: ignore[index]

    return GetOnlineFeaturesResponse.FeatureVector(
        values=values_output,
        statuses=statuses_output,
        event_timestamps=timestamp_output,
    )


def _get_aggregate_operations(agg_specs) -> dict:
    """
    Convert Aggregation specs to agg_ops format for PandasBackend.

    Reused from LocalFeatureBuilder logic.
    TODO: This logic is duplicated from feast.infra.compute_engines.local.feature_builder.LocalFeatureBuilder._get_aggregate_operations().
    Consider refactoring to a shared utility module in the future.
    """
    agg_ops = {}
    for agg in agg_specs:
        if agg.time_window is not None:
            raise ValueError(
                "Time window aggregation is not supported in online serving."
            )
        alias = f"{agg.function}_{agg.column}"
        agg_ops[alias] = (agg.function, agg.column)
    return agg_ops


def _apply_aggregations_to_response(
    response_data: Union[pyarrow.Table, Dict[str, List[Any]]],
    aggregations,
    group_keys: Optional[List[str]],
    mode: str,
) -> Union[pyarrow.Table, Dict[str, List[Any]]]:
    """
    Apply aggregations using PandasBackend.

    Args:
        response_data: Either a pyarrow.Table or dict of lists containing the data
        aggregations: List of Aggregation objects to apply
        group_keys: List of column names to group by (optional)
        mode: Transformation mode ("python", "pandas", or "substrait")

    Returns:
        Aggregated data in the same format as input

    TODO: Consider refactoring to support backends other than pandas in the future.
    """
    if not aggregations:
        return response_data

    backend = PandasBackend()

    # Convert to pandas DataFrame
    if isinstance(response_data, dict):
        df = pd.DataFrame(response_data)
    else:  # pyarrow.Table
        df = backend.from_arrow(response_data)

    if df.empty:
        return response_data

    # Convert aggregations to agg_ops format
    agg_ops = _get_aggregate_operations(aggregations)

    # Apply aggregations using PandasBackend
    if group_keys:
        result_df = backend.groupby_agg(df, group_keys, agg_ops)
    else:
        # No grouping - aggregate over entire dataset
        result_df = backend.groupby_agg(df, [], agg_ops)

    # Convert back to original format
    if mode == "python":
        return {col: result_df[col].tolist() for col in result_df.columns}
    else:  # pandas or substrait
        return backend.to_arrow(result_df)


def _augment_response_with_on_demand_transforms(
    online_features_response: GetOnlineFeaturesResponse,
    feature_refs: List[str],
    requested_on_demand_feature_views: List["OnDemandFeatureView"],
    full_feature_names: bool,
):
    """Computes on demand feature values and adds them to the result rows.

    Assumes that 'online_features_response' already contains the necessary request data and input feature
    views for the on demand feature views. Unneeded feature values such as request data and
    unrequested input feature views will be removed from 'online_features_response'.

    Args:
        online_features_response: Protobuf object to populate
        feature_refs: List of all feature references to be returned.
        requested_on_demand_feature_views: List of all odfvs that have been requested.
        full_feature_names: A boolean that provides the option to add the feature view prefixes to the feature names,
            changing them from the format "feature" to "feature_view__feature" (e.g., "daily_transactions" changes to
            "customer_fv__daily_transactions").
    """
    from feast.online_response import OnlineResponse

    requested_odfv_map = {odfv.name: odfv for odfv in requested_on_demand_feature_views}
    requested_odfv_feature_names = requested_odfv_map.keys()

    odfv_feature_refs = defaultdict(list)
    for feature_ref in feature_refs:
        view_name, feature_name = feature_ref.split(":")
        if view_name in requested_odfv_feature_names:
            odfv_feature_refs[view_name].append(
                f"{requested_odfv_map[view_name].projection.name_to_use()}__{feature_name}"
                if full_feature_names
                else feature_name
            )

    initial_response = OnlineResponse(online_features_response)
    initial_response_arrow: Optional[pyarrow.Table] = None
    initial_response_dict: Optional[Dict[str, List[Any]]] = None

    # Apply on demand transformations and augment the result rows
    odfv_result_names = set()
    for odfv_name, _feature_refs in odfv_feature_refs.items():
        odfv = requested_odfv_map[odfv_name]
        if not odfv.write_to_online_store:
            # Apply aggregations if configured.
            if odfv.aggregations:
                if odfv.mode == "python":
                    if initial_response_dict is None:
                        initial_response_dict = initial_response.to_dict()
                    initial_response_dict = _apply_aggregations_to_response(
                        initial_response_dict,
                        odfv.aggregations,
                        odfv.entities,
                        odfv.mode,
                    )
                elif odfv.mode in {"pandas", "substrait"}:
                    if initial_response_arrow is None:
                        initial_response_arrow = initial_response.to_arrow()
                    initial_response_arrow = _apply_aggregations_to_response(
                        initial_response_arrow,
                        odfv.aggregations,
                        odfv.entities,
                        odfv.mode,
                    )

            # Apply transformation. Note: aggregations and transformation configs are mutually exclusive
            # TODO: Fix to make it work for having both aggregation and transformation
            #  ticket: https://github.com/feast-dev/feast/issues/5689
            elif odfv.mode == "python":
                if initial_response_dict is None:
                    initial_response_dict = initial_response.to_dict()
                transformed_features_dict: Dict[str, List[Any]] = odfv.transform_dict(
                    initial_response_dict
                )
            elif odfv.mode in {"pandas", "substrait"}:
                if initial_response_arrow is None:
                    initial_response_arrow = initial_response.to_arrow()
                transformed_features_arrow = odfv.transform_arrow(
                    initial_response_arrow, full_feature_names
                )
            else:
                raise Exception(
                    f"Invalid OnDemandFeatureMode: {odfv.mode}. Expected one of 'pandas', 'python', or 'substrait'."
                )

            transformed_features = (
                transformed_features_dict
                if odfv.mode == "python"
                else transformed_features_arrow
            )
            transformed_columns = (
                transformed_features.column_names
                if isinstance(transformed_features, pyarrow.Table)
                else transformed_features
            )
            selected_subset = [f for f in transformed_columns if f in _feature_refs]

            proto_values = []
            schema_dict = {k.name: k.dtype for k in odfv.schema}
            for selected_feature in selected_subset:
                feature_vector = transformed_features[selected_feature]
                selected_feature_type = schema_dict.get(selected_feature, None)
                feature_type: ValueType = ValueType.UNKNOWN
                if selected_feature_type is not None:
                    if isinstance(
                        selected_feature_type, (ComplexFeastType, PrimitiveFeastType)
                    ):
                        feature_type = selected_feature_type.to_value_type()
                    elif not isinstance(selected_feature_type, ValueType):
                        raise TypeError(
                            f"Unexpected type for feature_type: {type(feature_type)}"
                        )

                proto_values.append(
                    python_values_to_proto_values(
                        feature_vector
                        if isinstance(feature_vector, list)
                        else [feature_vector]
                        if odfv.mode == "python"
                        else feature_vector.to_numpy(),
                        feature_type,
                    )
                )

            odfv_result_names |= set(selected_subset)

            online_features_response.metadata.feature_names.val.extend(selected_subset)
            for feature_idx in range(len(selected_subset)):
                online_features_response.results.append(
                    GetOnlineFeaturesResponse.FeatureVector(
                        values=proto_values[feature_idx],
                        statuses=[FieldStatus.PRESENT] * len(proto_values[feature_idx]),
                        event_timestamps=[Timestamp()] * len(proto_values[feature_idx]),
                    )
                )


def _get_entity_maps(
    registry,
    project,
    feature_views,
) -> Tuple[Dict[str, str], Dict[str, ValueType], Set[str]]:
    # TODO(felixwang9817): Support entities that have different types for different feature views.
    entities = registry.list_entities(project, allow_cache=True)
    entity_name_to_join_key_map: Dict[str, str] = {}
    entity_type_map: Dict[str, ValueType] = {}
    for entity in entities:
        entity_name_to_join_key_map[entity.name] = entity.join_key
    for feature_view in feature_views:
        for entity_name in feature_view.entities:
            entity = registry.get_entity(entity_name, project, allow_cache=True)
            # User directly uses join_key as the entity reference in the entity_rows for the
            # entity mapping case.
            entity_name = feature_view.projection.join_key_map.get(
                entity.join_key, entity.name
            )
            join_key = feature_view.projection.join_key_map.get(
                entity.join_key, entity.join_key
            )
            entity_name_to_join_key_map[entity_name] = join_key

        for entity_column in feature_view.entity_columns:
            dtype = entity_column.dtype.to_value_type()
            entity_join_key_column_name = feature_view.projection.join_key_map.get(
                entity_column.name, entity_column.name
            )
            entity_type_map[entity_join_key_column_name] = dtype

    return (
        entity_name_to_join_key_map,
        entity_type_map,
        set(entity_name_to_join_key_map.values()),
    )


def _get_table_entity_values(
    table: "FeatureView",
    entity_name_to_join_key_map: Dict[str, str],
    join_key_proto_values: Dict[str, List[ValueProto]],
) -> Dict[str, List[ValueProto]]:
    # The correct join_keys expected by the OnlineStore for this Feature View.
    table_join_keys = [
        entity_name_to_join_key_map[entity_name] for entity_name in table.entities
    ]

    # If the FeatureView has a Projection then the join keys may be aliased.
    alias_to_join_key_map = {v: k for k, v in table.projection.join_key_map.items()}

    # Subset to columns which are relevant to this FeatureView and
    # give them the correct names.
    entity_values = {
        alias_to_join_key_map.get(k, k): v
        for k, v in join_key_proto_values.items()
        if alias_to_join_key_map.get(k, k) in table_join_keys
    }
    return entity_values


def _get_unique_entities(
    table: "FeatureView",
    join_key_values: Dict[str, List[ValueProto]],
    entity_name_to_join_key_map: Dict[str, str],
) -> Tuple[Tuple[Dict[str, ValueProto], ...], Tuple[List[int], ...], int]:
    """Return the set of unique composite Entities for a Feature View and the indexes at which they appear.

    This method allows us to query the OnlineStore for data we need only once
    rather than requesting and processing data for the same combination of
    Entities multiple times.
    """
    # Get the correct set of entity values with the correct join keys.
    table_entity_values = _get_table_entity_values(
        table,
        entity_name_to_join_key_map,
        join_key_values,
    )
    # Validate that all expected join keys exist and have non-empty values.
    expected_keys = set(entity_name_to_join_key_map.values())
    expected_keys.discard("__dummy_id")
    missing_keys = sorted(
        list(set([key for key in expected_keys if key not in table_entity_values]))
    )
    empty_keys = sorted(
        list(set([key for key in expected_keys if not table_entity_values.get(key)]))
    )

    if missing_keys or empty_keys:
        if not any(table_entity_values.values()):
            raise KeyError(
                f"Missing join key values for keys: {missing_keys}. "
                f"No values provided for keys: {empty_keys}. "
                f"Provided join_key_values: {list(join_key_values.keys())}"
            )

    # Convert the column-oriented table_entity_values into row-wise data.
    keys = list(table_entity_values.keys())
    # Each row is a tuple of ValueProto objects corresponding to the join keys.
    rowise = list(enumerate(zip(*table_entity_values.values())))

    # If there are no rows, return empty tuples.
    if not rowise:
        return (), (), 0

    # Sort rowise so that rows with the same join key values are adjacent.
    rowise.sort(key=lambda row: tuple(getattr(x, x.WhichOneof("val")) for x in row[1]))

    # Group rows by their composite join key value.
    groups = [
        (dict(zip(keys, key_tuple)), [idx for idx, _ in group])
        for key_tuple, group in itertools.groupby(rowise, key=lambda row: row[1])
    ]

    # If no groups were formed (should not happen for valid input), return empty tuples.
    if not groups:
        return (), (), 0

    # Unpack the unique entities and their original row indexes.
    unique_entities, indexes = tuple(zip(*groups))
    return unique_entities, indexes, len(rowise)


def _get_unique_entities_from_values(
    table_entity_values: Dict[str, List[ValueProto]],
) -> Tuple[Tuple[Dict[str, ValueProto], ...], Tuple[List[int], ...], int]:
    """Return the set of unique composite Entities for a Feature View and the indexes at which they appear.

    This method allows us to query the OnlineStore for data we need only once
    rather than requesting and processing data for the same combination of
    Entities multiple times.
    """
    keys = table_entity_values.keys()
    # Sort the rowise data to allow for grouping but keep original index. This lambda is
    # sufficient as Entity types cannot be complex (ie. lists).
    rowise = list(enumerate(zip(*table_entity_values.values())))
    rowise.sort(key=lambda row: tuple(getattr(x, x.WhichOneof("val")) for x in row[1]))

    # Identify unique entities and the indexes at which they occur.
    unique_entities: Tuple[Dict[str, ValueProto], ...]
    indexes: Tuple[List[int], ...]
    unique_entities, indexes = tuple(
        zip(
            *[
                (dict(zip(keys, k)), [_[0] for _ in g])
                for k, g in itertools.groupby(rowise, key=lambda x: x[1])
            ]
        )
    )
    return unique_entities, indexes, len(rowise)


def _drop_unneeded_columns(
    online_features_response: GetOnlineFeaturesResponse,
    requested_result_row_names: Set[str],
):
    """
    Unneeded feature values such as request data and unrequested input feature views will
    be removed from 'online_features_response'.

    Args:
        online_features_response: Protobuf object to populate
        requested_result_row_names: Fields from 'result_rows' that have been requested, and
                therefore should not be dropped.
    """
    # Drop values that aren't needed
    unneeded_feature_indices = [
        idx
        for idx, val in enumerate(online_features_response.metadata.feature_names.val)
        if val not in requested_result_row_names
    ]

    for idx in reversed(unneeded_feature_indices):
        del online_features_response.metadata.feature_names.val[idx]
        del online_features_response.results[idx]


def _populate_result_rows_from_columnar(
    online_features_response: GetOnlineFeaturesResponse,
    data: Dict[str, List[ValueProto]],
):
    timestamp = Timestamp()  # Only initialize this timestamp once.
    # Add more values to the existing result rows
    for feature_name, feature_values in data.items():
        online_features_response.metadata.feature_names.val.append(feature_name)
        online_features_response.results.append(
            GetOnlineFeaturesResponse.FeatureVector(
                values=feature_values,
                statuses=[FieldStatus.PRESENT] * len(feature_values),
                event_timestamps=[timestamp] * len(feature_values),
            )
        )


def get_needed_request_data(
    grouped_odfv_refs: List[Tuple["OnDemandFeatureView", List[str]]],
) -> Set[str]:
    needed_request_data: Set[str] = set()
    for odfv, _ in grouped_odfv_refs:
        odfv_request_data_schema = odfv.get_request_data_schema()
        # if odfv.write_to_online_store, we should not pass in the request data
        needed_request_data.update(odfv_request_data_schema.keys())
    return needed_request_data


def ensure_request_data_values_exist(
    needed_request_data: Set[str],
    request_data_features: Dict[str, List[Any]],
):
    if len(needed_request_data) != len(request_data_features.keys()):
        missing_features = [
            x for x in needed_request_data if x not in request_data_features
        ]
        raise RequestDataNotFoundInEntityRowsException(feature_names=missing_features)


def _populate_response_from_feature_data(
    feature_data: Iterable[
        Tuple[
            Iterable[Timestamp], Iterable["FieldStatus.ValueType"], Iterable[ValueProto]
        ]
    ],
    indexes: Iterable[List[int]],
    online_features_response: GetOnlineFeaturesResponse,
    full_feature_names: bool,
    requested_features: Iterable[str],
    table: "FeatureView",
    output_len: int,
):
    """Populate the GetOnlineFeaturesResponse with feature data.

    This method assumes that `_read_from_online_store` returns data for each
    combination of Entities in `entity_rows` in the same order as they
    are provided.

    Args:
        feature_data: A list of data in Protobuf form which was retrieved from the OnlineStore.
        indexes: A list of indexes which should be the same length as `feature_data`. Each list
            of indexes corresponds to a set of result rows in `online_features_response`.
        online_features_response: The object to populate.
        full_feature_names: A boolean that provides the option to add the feature view prefixes to the feature names,
            changing them from the format "feature" to "feature_view__feature" (e.g., "daily_transactions" changes to
            "customer_fv__daily_transactions").
        requested_features: The names of the features in `feature_data`. This should be ordered in the same way as the
            data in `feature_data`.
        table: The FeatureView that `feature_data` was retrieved from.
        output_len: The number of result rows in `online_features_response`.
    """
    # Add the feature names to the response.
    table_name = table.projection.name_to_use()
    requested_feature_refs = [
        f"{table_name}__{feature_name}" if full_feature_names else feature_name
        for feature_name in requested_features
    ]
    online_features_response.metadata.feature_names.val.extend(requested_feature_refs)

    # Process each feature vector in a single pass
    for timestamp_vector, statuses_vector, values_vector in feature_data:
        response_vector = construct_response_feature_vector(
            values_vector, statuses_vector, timestamp_vector, indexes, output_len
        )
        online_features_response.results.append(response_vector)


def _populate_response_from_feature_data_v2(
    feature_data: Iterable[
        Tuple[
            Iterable[Timestamp], Iterable["FieldStatus.ValueType"], Iterable[ValueProto]
        ]
    ],
    indexes: Iterable[List[int]],
    online_features_response: GetOnlineFeaturesResponse,
    requested_features: Iterable[str],
    output_len: int,
):
    """Populate the GetOnlineFeaturesResponse with feature data.

    This method assumes that `_read_from_online_store` returns data for each
    combination of Entities in `entity_rows` in the same order as they
    are provided.

    Args:
        feature_data: A list of data in Protobuf form which was retrieved from the OnlineStore.
        indexes: A list of indexes which should be the same length as `feature_data`. Each list
            of indexes corresponds to a set of result rows in `online_features_response`.
        online_features_response: The object to populate.
        full_feature_names: A boolean that provides the option to add the feature view prefixes to the feature names,
            changing them from the format "feature" to "feature_view__feature" (e.g., "daily_transactions" changes to
            "customer_fv__daily_transactions").
        requested_features: The names of the features in `feature_data`. This should be ordered in the same way as the
            data in `feature_data`.
        output_len: The number of result rows in `online_features_response`.
    """
    # Add the feature names to the response.
    requested_feature_refs = [(feature_name) for feature_name in requested_features]
    online_features_response.metadata.feature_names.val.extend(requested_feature_refs)

    timestamps, statuses, values = zip(*feature_data)

    # Populate the result with data fetched from the OnlineStore
    # which is guaranteed to be aligned with `requested_features`.
    for timestamp_vector, statuses_vector, values_vector in feature_data:
        response_vector = construct_response_feature_vector(
            values_vector, statuses_vector, timestamp_vector, indexes, output_len
        )
        online_features_response.results.append(response_vector)


def _convert_entity_key_to_proto_to_dict(
    entity_key_vals: List[EntityKeyProto],
) -> Dict[str, List[ValueProto]]:
    entity_dict: Dict[str, List[ValueProto]] = {}
    for entity_key_val in entity_key_vals:
        if entity_key_val is not None:
            for join_key, entity_value in zip(
                entity_key_val.join_keys, entity_key_val.entity_values
            ):
                if join_key not in entity_dict:
                    entity_dict[join_key] = []
                    # python_entity_value = _proto_value_to_value_type(entity_value)
                    entity_dict[join_key].append(entity_value)
    return entity_dict


def _get_features(
    registry,
    project,
    features: Union[List[str], "FeatureService"],
    allow_cache: bool = False,
) -> List[str]:
    from feast.feature_service import FeatureService

    _features = features

    if not _features:
        raise ValueError("No features specified for retrieval")

    _feature_refs = []
    if isinstance(_features, FeatureService):
        feature_service_from_registry = registry.get_feature_service(
            _features.name, project, allow_cache
        )
        if feature_service_from_registry != _features:
            warnings.warn(
                "The FeatureService object that has been passed in as an argument is "
                "inconsistent with the version from the registry. Potentially a newer version "
                "of the FeatureService has been applied to the registry."
            )
        for projection in feature_service_from_registry.feature_view_projections:
            _feature_refs.extend(
                [f"{projection.name_to_use()}:{f.name}" for f in projection.features]
            )
    else:
        assert isinstance(_features, list)
        _feature_refs = _features
    return _feature_refs


def _list_feature_views(
    registry,
    project,
    allow_cache: bool = False,
    hide_dummy_entity: bool = True,
    tags: Optional[dict[str, str]] = None,
) -> List["FeatureView"]:
    from feast.feature_view import DUMMY_ENTITY_NAME

    feature_views = []
    for fv in registry.list_feature_views(project, allow_cache=allow_cache, tags=tags):
        if hide_dummy_entity and fv.entities and fv.entities[0] == DUMMY_ENTITY_NAME:
            fv.entities = []
            fv.entity_columns = []
        feature_views.append(fv)
    return feature_views


def _get_feature_views_to_use(
    registry: "BaseRegistry",
    project,
    features: Union[List[str], "FeatureService"],
    allow_cache=False,
    hide_dummy_entity: bool = True,
) -> Tuple[List["FeatureView"], List["OnDemandFeatureView"]]:
    from feast.feature_service import FeatureService
    from feast.feature_view import DUMMY_ENTITY_NAME
    from feast.on_demand_feature_view import OnDemandFeatureView

    if isinstance(features, FeatureService):
        feature_views = [
            (projection.name, projection)
            for projection in features.feature_view_projections
        ]
    else:
        assert features is not None
        feature_views = [(feature.split(":")[0], None) for feature in features]  # type: ignore[misc]

    fvs_to_use, od_fvs_to_use = [], []
    for name, projection in feature_views:
        fv = registry.get_any_feature_view(name, project, allow_cache)

        if isinstance(fv, OnDemandFeatureView):
            od_fvs_to_use.append(
                fv.with_projection(copy.copy(projection)) if projection else fv
            )

            for source_projection in fv.source_feature_view_projections.values():
                source_fv = registry.get_any_feature_view(
                    source_projection.name, project, allow_cache
                )
                # TODO better way to handler dummy entities
                if (
                    hide_dummy_entity
                    and source_fv.entities  # type: ignore[attr-defined]
                    and source_fv.entities[0] == DUMMY_ENTITY_NAME  # type: ignore[attr-defined]
                ):
                    source_fv.entities = []  # type: ignore[attr-defined]
                    source_fv.entity_columns = []  # type: ignore[attr-defined]

                if source_fv not in fvs_to_use:
                    fvs_to_use.append(
                        source_fv.with_projection(copy.copy(source_projection))
                    )
        else:
            if (
                hide_dummy_entity
                and fv.entities  # type: ignore[attr-defined]
                and fv.entities[0] == DUMMY_ENTITY_NAME  # type: ignore[attr-defined]
            ):
                fv.entities = []  # type: ignore[attr-defined]
                fv.entity_columns = []  # type: ignore[attr-defined]
            fvs_to_use.append(
                fv.with_projection(copy.copy(projection)) if projection else fv
            )

    return (fvs_to_use, od_fvs_to_use)


def _get_online_request_context(
    registry,
    project,
    features: Union[List[str], "FeatureService"],
    full_feature_names: bool,
):
    from feast.feature_view import DUMMY_ENTITY_NAME

    _feature_refs = _get_features(registry, project, features, allow_cache=True)

    (
        requested_feature_views,
        requested_on_demand_feature_views,
    ) = _get_feature_views_to_use(
        registry=registry,
        project=project,
        features=features,
        allow_cache=True,
        hide_dummy_entity=False,
    )

    (
        entity_name_to_join_key_map,
        entity_type_map,
        join_keys_set,
    ) = _get_entity_maps(registry, project, requested_feature_views)

    _validate_feature_refs(_feature_refs, full_feature_names)
    (
        grouped_refs,
        grouped_odfv_refs,
    ) = _group_feature_refs(
        _feature_refs,
        requested_feature_views,
        requested_on_demand_feature_views,
    )

    requested_result_row_names = {
        feat_ref.replace(":", "__") for feat_ref in _feature_refs
    }
    if not full_feature_names:
        requested_result_row_names = {
            name.rpartition("__")[-1] for name in requested_result_row_names
        }

    feature_views = list(view for view, _ in grouped_refs)

    needed_request_data = get_needed_request_data(grouped_odfv_refs)

    entityless_case = DUMMY_ENTITY_NAME in [
        entity_name
        for feature_view in feature_views
        for entity_name in (feature_view.entities or [])
    ]

    return (
        _feature_refs,
        requested_on_demand_feature_views,
        entity_name_to_join_key_map,
        entity_type_map,
        join_keys_set,
        grouped_refs,
        requested_result_row_names,
        needed_request_data,
        entityless_case,
    )


def _prepare_entities_to_read_from_online_store(
    registry,
    project,
    features: Union[List[str], "FeatureService"],
    entity_values: Mapping[
        str, Union[Sequence[Any], Sequence[ValueProto], RepeatedValueProto]
    ],
    full_feature_names: bool = False,
    native_entity_values: bool = True,
):
    from feast.feature_view import DUMMY_ENTITY, DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL

    (
        feature_refs,
        requested_on_demand_feature_views,
        entity_name_to_join_key_map,
        entity_type_map,
        join_keys_set,
        grouped_refs,
        requested_result_row_names,
        needed_request_data,
        entityless_case,
    ) = _get_online_request_context(registry, project, features, full_feature_names)

    # Extract Sequence from RepeatedValue Protobuf.
    entity_value_lists: Dict[str, Union[List[Any], List[ValueProto]]] = {
        k: list(v) if isinstance(v, Sequence) else list(v.val)
        for k, v in entity_values.items()
    }

    entity_proto_values: Dict[str, List[ValueProto]]
    if native_entity_values:
        # Convert values to Protobuf once.
        entity_proto_values = {
            k: python_values_to_proto_values(
                v, entity_type_map.get(k, ValueType.UNKNOWN)
            )
            for k, v in entity_value_lists.items()
        }
    else:
        entity_proto_values = entity_value_lists

    num_rows = _validate_entity_values(entity_proto_values)

    odfv_entities: List[Entity] = []
    request_source_keys: List[str] = []
    for on_demand_feature_view in requested_on_demand_feature_views:
        entities_for_odfv = getattr(on_demand_feature_view, "entities", [])
        if len(entities_for_odfv) > 0 and isinstance(entities_for_odfv[0], str):
            entities_for_odfv = [
                registry.get_entity(entity_name, project, allow_cache=True)
                for entity_name in entities_for_odfv
            ]
        odfv_entities.extend(entities_for_odfv)
        for source in on_demand_feature_view.source_request_sources:
            source_schema = on_demand_feature_view.source_request_sources[source].schema
            for column in source_schema:
                request_source_keys.append(column.name)

    join_keys_set.update(set(odfv_entities))

    join_key_values: Dict[str, List[ValueProto]] = {}
    request_data_features: Dict[str, List[ValueProto]] = {}
    # Entity rows may be either entities or request data.
    for join_key_or_entity_name, values in entity_proto_values.items():
        # Found request data
        if join_key_or_entity_name in needed_request_data:
            request_data_features[join_key_or_entity_name] = values
        elif join_key_or_entity_name in join_keys_set:
            # It's a join key
            join_key = join_key_or_entity_name
            requested_result_row_names.add(join_key)
            join_key_values[join_key] = values
        elif join_key_or_entity_name in entity_name_to_join_key_map:
            # It's an entity name (deprecated)
            join_key = entity_name_to_join_key_map[join_key_or_entity_name]
            warnings.warn("Using entity name is deprecated. Use join_key instead.")
            requested_result_row_names.add(join_key)
            join_key_values[join_key] = values
        else:
            # Key is not recognized (likely a feature value), so we skip it.
            continue  # Or handle accordingly

    ensure_request_data_values_exist(needed_request_data, request_data_features)

    # Populate online features response proto with join keys and request data features
    online_features_response = GetOnlineFeaturesResponse(results=[])
    _populate_result_rows_from_columnar(
        online_features_response=online_features_response,
        data=dict(**join_key_values, **request_data_features),
    )

    # Add the Entityless case after populating result rows to avoid having to remove
    # it later.
    if entityless_case:
        join_key_values[DUMMY_ENTITY_ID] = python_values_to_proto_values(
            [DUMMY_ENTITY_VAL] * num_rows, DUMMY_ENTITY.value_type
        )

    return (
        join_key_values,
        grouped_refs,
        entity_name_to_join_key_map,
        requested_on_demand_feature_views,
        feature_refs,
        requested_result_row_names,
        online_features_response,
    )


def _get_entity_key_protos(
    entity_rows: Iterable[Mapping[str, ValueProto]],
) -> List[EntityKeyProto]:
    # Instantiate one EntityKeyProto per Entity.
    entity_key_protos = [
        EntityKeyProto(join_keys=row.keys(), entity_values=row.values())
        for row in entity_rows
    ]
    return entity_key_protos


def _convert_rows_to_protobuf(
    requested_features: List[str],
    read_rows: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]],
) -> List[Tuple[List[Timestamp], List["FieldStatus.ValueType"], List[ValueProto]]]:
    # Pre-calculate the length to avoid repeated calculations
    n_rows = len(read_rows)

    # Create single instances of commonly used values
    null_value = ValueProto()
    null_status = FieldStatus.NOT_FOUND
    null_timestamp = Timestamp()
    present_status = FieldStatus.PRESENT

    requested_features_vectors = []
    for feature_name in requested_features:
        ts_vector = [null_timestamp] * n_rows
        status_vector = [null_status] * n_rows
        value_vector = [null_value] * n_rows
        for idx, read_row in enumerate(read_rows):
            row_ts_proto = Timestamp()
            row_ts, feature_data = read_row
            # TODO (Ly): reuse whatever timestamp if row_ts is None?
            if row_ts is not None:
                row_ts_proto.FromDatetime(row_ts)
            ts_vector[idx] = row_ts_proto
            if (feature_data is not None) and (feature_name in feature_data):
                status_vector[idx] = present_status
                value_vector[idx] = feature_data[feature_name]
        requested_features_vectors.append((ts_vector, status_vector, value_vector))
    return requested_features_vectors


def has_all_tags(
    object_tags: dict[str, str], requested_tags: Optional[dict[str, str]] = None
) -> bool:
    if requested_tags is None:
        return True
    return all(object_tags.get(key, None) == val for key, val in requested_tags.items())


def tags_list_to_dict(
    tags_list: Optional[list[str]] = None,
) -> Optional[dict[str, str]]:
    if not tags_list:
        return None
    tags_dict: dict[str, str] = {}
    for tags_str in tags_list:
        tags_dict.update(tags_str_to_dict(tags_str))
    return tags_dict


def tags_str_to_dict(tags: str = "") -> dict[str, str]:
    tags_list = tags.strip().strip("()").replace('"', "").replace("'", "").split(",")
    return {
        key.strip(): value.strip()
        for key, value in dict(
            cast(tuple[str, str], tag.split(":", 1)) for tag in tags_list if ":" in tag
        ).items()
    }


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _serialize_vector_to_float_list(vector: List[float]) -> ValueProto:
    return ValueProto(float_list_val=FloatListProto(val=vector))


def _build_retrieve_online_document_record(
    entity_key: Union[str, bytes],
    feature_value: Union[str, bytes],
    vector_value: Union[str, List[float]],
    distance_value: float,
    event_timestamp: datetime,
    entity_key_serialization_version: int,
) -> Tuple[
    Optional[datetime],
    Optional[EntityKeyProto],
    Optional[ValueProto],
    Optional[ValueProto],
    Optional[ValueProto],
]:
    if entity_key_serialization_version < 3:
        entity_key_proto = None
    else:
        if isinstance(entity_key, str):
            entity_key_proto_bin = entity_key.encode("utf-8")
        else:
            entity_key_proto_bin = entity_key
        entity_key_proto = deserialize_entity_key(
            entity_key_proto_bin,
            entity_key_serialization_version=entity_key_serialization_version,
        )

    feature_value_proto = ValueProto()

    if isinstance(feature_value, str):
        feature_value_proto.ParseFromString(feature_value.encode("utf-8"))
    else:
        feature_value_proto.ParseFromString(feature_value)

    if isinstance(vector_value, str):
        vector_value_proto = ValueProto(string_val=vector_value)
    else:
        vector_value_proto = ValueProto(float_list_val=FloatListProto(val=vector_value))

    distance_value_proto = ValueProto(float_val=distance_value)
    return (
        event_timestamp,
        entity_key_proto,
        feature_value_proto,
        vector_value_proto,
        distance_value_proto,
    )


def _get_feature_view_vector_field_metadata(
    feature_view,
) -> Optional[Field]:
    vector_fields = [field for field in feature_view.schema if field.vector_index]
    if len(vector_fields) > 1:
        raise ValueError(
            f"Feature view {feature_view.name} has multiple vector fields. Only one vector field per feature view is supported."
        )
    if not vector_fields:
        return None
    return vector_fields[0]
