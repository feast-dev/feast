import re
from typing import List, Optional, Set, Union

from feast.data_source import DataSource, PushSource, RequestSource
from feast.entity import Entity
from feast.errors import RegistryInferenceFailure
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_NAME, FeatureView
from feast.field import Field, from_value_type
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source import (
    MsSqlServerSource,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.infra.provider import Provider
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import String
from feast.value_type import ValueType


def update_data_sources_with_inferred_event_timestamp_col(
    data_sources: List[DataSource], config: RepoConfig
) -> None:
    ERROR_MSG_PREFIX = "Unable to infer DataSource timestamp_field"
    for data_source in data_sources:
        if data_source is None:
            continue
        if isinstance(data_source, RequestSource):
            continue
        if isinstance(data_source, PushSource):
            if not isinstance(data_source.batch_source, DataSource):
                continue
            else:
                data_source = data_source.batch_source
        if data_source.timestamp_field is None or data_source.timestamp_field == "":
            # prepare right match pattern for data source
            ts_column_type_regex_pattern: str
            # TODO(adchia): Move Spark source inference out of this logic
            if (
                isinstance(data_source, FileSource)
                or "SparkSource" == data_source.__class__.__name__
            ):
                ts_column_type_regex_pattern = r"^timestamp"
            elif isinstance(data_source, BigQuerySource):
                ts_column_type_regex_pattern = "TIMESTAMP|DATETIME"
            elif isinstance(data_source, RedshiftSource):
                ts_column_type_regex_pattern = "TIMESTAMP[A-Z]*"
            elif isinstance(data_source, SnowflakeSource):
                ts_column_type_regex_pattern = "TIMESTAMP_[A-Z]*"
            elif isinstance(data_source, MsSqlServerSource):
                ts_column_type_regex_pattern = "TIMESTAMP|DATETIME"
            else:
                raise RegistryInferenceFailure(
                    "DataSource",
                    f"""
                    DataSource inferencing of timestamp_field is currently only supported
                    for FileSource, SparkSource, BigQuerySource, RedshiftSource, SnowflakeSource, MsSqlSource.
                    Attempting to infer from {data_source}.
                    """,
                )
            #  for informing the type checker
            assert (
                isinstance(data_source, FileSource)
                or isinstance(data_source, BigQuerySource)
                or isinstance(data_source, RedshiftSource)
                or isinstance(data_source, SnowflakeSource)
                or isinstance(data_source, MsSqlServerSource)
                or "SparkSource" == data_source.__class__.__name__
            )

            # loop through table columns to find singular match
            timestamp_fields = []
            for (
                col_name,
                col_datatype,
            ) in data_source.get_table_column_names_and_types(config):
                if re.match(ts_column_type_regex_pattern, col_datatype):
                    timestamp_fields.append(col_name)

            if len(timestamp_fields) > 1:
                raise RegistryInferenceFailure(
                    "DataSource",
                    f"""{ERROR_MSG_PREFIX}; found multiple possible columns of timestamp type.
                    Data source type: {data_source.__class__.__name__},
                    Timestamp regex: `{ts_column_type_regex_pattern}`, columns: {timestamp_fields}""",
                )
            elif len(timestamp_fields) == 1:
                data_source.timestamp_field = timestamp_fields[0]
            else:
                raise RegistryInferenceFailure(
                    "DataSource",
                    f"""
                    {ERROR_MSG_PREFIX}; Found no columns of timestamp type.
                    Data source type: {data_source.__class__.__name__},
                    Timestamp regex: `{ts_column_type_regex_pattern}`.
                    """,
                )


def update_feature_views_with_inferred_features_and_entities(
    provider: Provider,
    fvs: Union[List[FeatureView], List[StreamFeatureView], List[OnDemandFeatureView]],
    entities: List[Entity],
    config: RepoConfig,
) -> None:
    """
    Infers the features and entities associated with each feature view and updates it in place.

    Columns whose names match a join key of an entity are considered to be entity columns; all
    other columns except designated timestamp columns are considered to be feature columns. If
    the feature view already has features, feature inference is skipped.

    Note that this inference logic currently does not take any transformations (either a UDF or
    aggregations) into account. For example, even if a stream feature view has a transformation,
    this method assumes that the batch source contains transformed data with the correct final schema.

    Args:
        fvs: The feature views to be updated.
        entities: A list containing entities associated with the feature views.
        config: The config for the current feature store.
    """
    entity_name_to_entity_map = {e.name: e for e in entities}
    entity_name_to_join_key_map = {e.name: e.join_key for e in entities}

    for fv in fvs:
        join_keys = set(
            [
                entity_name_to_join_key_map.get(entity_name)
                for entity_name in getattr(fv, "entities", [])
            ]
        )

        # Fields whose names match a join key are considered to be entity columns; all
        # other fields are considered to be feature columns.
        entity_columns = fv.entity_columns if fv.entity_columns else []
        for field in fv.schema:
            if field.name in join_keys:
                # Do not override a preexisting field with the same name.
                if field.name not in [
                    entity_column.name for entity_column in entity_columns
                ]:
                    entity_columns.append(field)
            else:
                if field.name not in [feature.name for feature in fv.features]:
                    fv.features.append(field)

        # Respect the `value_type` attribute of the entity, if it is specified.
        fv_entities = getattr(fv, "entities", [])
        for entity_name in fv_entities:
            entity = entity_name_to_entity_map.get(entity_name)
            # pass when entity does not exist. Entityless feature view case
            if entity is None:
                continue
            if (
                entity.join_key
                not in [entity_column.name for entity_column in entity_columns]
                and entity.value_type != ValueType.UNKNOWN
            ):
                entity_columns.append(
                    Field(
                        name=entity.join_key,
                        dtype=from_value_type(entity.value_type),
                    )
                )

        # Infer a dummy entity column for entityless feature views.
        if (
            len(fv_entities) == 1
            and fv_entities[0] == DUMMY_ENTITY_NAME
            and not entity_columns
        ):
            entity_columns.append(Field(name=DUMMY_ENTITY_ID, dtype=String))

        fv.entity_columns = entity_columns
        # Run inference for entity columns if there are fewer entity fields than expected.
        run_inference_for_entities = len(fv.entity_columns) < len(join_keys)

        # Run inference for feature columns if there are no feature fields.
        run_inference_for_features = len(fv.features) == 0

        if run_inference_for_entities or run_inference_for_features:
            _infer_features_and_entities(
                provider,
                fv,
                join_keys,
                run_inference_for_features,
                config,
            )

            if not fv.features:
                if isinstance(fv, OnDemandFeatureView):
                    return None
                else:
                    raise RegistryInferenceFailure(
                        "FeatureView",
                        f"Could not infer Features for the FeatureView named {fv.name}.",
                    )


def _infer_features_and_entities(
    provider: Provider,
    fv: Union[FeatureView, OnDemandFeatureView],
    join_keys: Set[Optional[str]],
    run_inference_for_features,
    config,
) -> None:
    """
    Updates the specific feature in place with inferred features and entities.

    Args:
        fv: The feature view on which to run inference.
        join_keys: The set of join keys for the feature view's entities.
        run_inference_for_features: Whether to run inference for features.
        config: The config for the current feature store.
    """
    if isinstance(fv, OnDemandFeatureView):
        return _infer_on_demand_features_and_entities(
            fv, join_keys, run_inference_for_features, config
        )

    entity_columns: List[Field] = fv.entity_columns if fv.entity_columns else []
    columns_to_exclude = {
        fv.batch_source.timestamp_field,
        fv.batch_source.created_timestamp_column,
    }
    for original_col, mapped_col in fv.batch_source.field_mapping.items():
        if mapped_col in columns_to_exclude:
            columns_to_exclude.remove(mapped_col)
            columns_to_exclude.add(original_col)

    table_column_names_and_types = (
        provider.get_table_column_names_and_types_from_data_source(
            config, fv.batch_source
        )
    )

    for col_name, col_datatype in table_column_names_and_types:
        if col_name in columns_to_exclude:
            continue
        elif col_name in join_keys:
            field = Field(
                name=col_name,
                dtype=from_value_type(
                    fv.batch_source.source_datatype_to_feast_value_type()(col_datatype)
                ),
            )
            if field.name not in [
                entity_column.name for entity_column in fv.entity_columns
            ]:
                entity_columns.append(field)
        elif not re.match(
            "^__|__$", col_name
        ):  # double underscores often signal an internal-use column
            if run_inference_for_features:
                feature_name = (
                    fv.batch_source.field_mapping[col_name]
                    if col_name in fv.batch_source.field_mapping
                    else col_name
                )
                field = Field(
                    name=feature_name,
                    dtype=from_value_type(
                        fv.batch_source.source_datatype_to_feast_value_type()(
                            col_datatype
                        )
                    ),
                )
                if field.name not in [feature.name for feature in fv.features]:
                    fv.features.append(field)

    fv.entity_columns = entity_columns


def _infer_on_demand_features_and_entities(
    fv: OnDemandFeatureView,
    join_keys: Set[Optional[str]],
    run_inference_for_features,
    config,
) -> None:
    """
    Updates the specific feature in place with inferred features and entities.
    Args:
        fv: The feature view on which to run inference.
        join_keys: The set of join keys for the feature view's entities.
        run_inference_for_features: Whether to run inference for features.
        config: The config for the current feature store.
    """
    entity_columns: list[Field] = []
    columns_to_exclude = set()
    for (
        source_feature_view_name,
        source_feature_view,
    ) in fv.source_feature_view_projections.items():
        columns_to_exclude.add(source_feature_view.timestamp_field)
        columns_to_exclude.add(source_feature_view.created_timestamp_column)

        batch_source = getattr(source_feature_view, "batch_source")
        batch_field_mapping = getattr(batch_source or None, "field_mapping")
        for (
            original_col,
            mapped_col,
        ) in batch_field_mapping.items():
            if mapped_col in columns_to_exclude:
                columns_to_exclude.remove(mapped_col)
                columns_to_exclude.add(original_col)

        table_column_names_and_types = batch_source.get_table_column_names_and_types(
            config
        )
        batch_field_mapping = getattr(batch_source, "field_mapping", {})

        for col_name, col_datatype in table_column_names_and_types:
            if col_name in columns_to_exclude:
                continue
            elif col_name in join_keys:
                field = Field(
                    name=col_name,
                    dtype=from_value_type(
                        batch_source.source_datatype_to_feast_value_type()(col_datatype)
                    ),
                )
                if field.name not in [
                    entity_column.name
                    for entity_column in entity_columns
                    if hasattr(entity_column, "name")
                ]:
                    entity_columns.append(field)
            elif not re.match(
                "^__|__$", col_name
            ):  # double underscores often signal an internal-use column
                if run_inference_for_features:
                    feature_name = (
                        batch_field_mapping[col_name]
                        if col_name in batch_field_mapping
                        else col_name
                    )
                    field = Field(
                        name=feature_name,
                        dtype=from_value_type(
                            batch_source.source_datatype_to_feast_value_type()(
                                col_datatype
                            )
                        ),
                    )
                    if field.name not in [
                        feature.name for feature in source_feature_view.features
                    ]:
                        source_feature_view.features.append(field)
    fv.entity_columns = entity_columns
