import re
from typing import List, Set, Union

from feast.data_source import DataSource, PushSource, RequestSource
from feast.entity import Entity
from feast.errors import RegistryInferenceFailure
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_NAME, FeatureView
from feast.field import Field, from_value_type
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import String
from feast.value_type import ValueType


def update_data_sources_with_inferred_event_timestamp_col(
    data_sources: List[DataSource], config: RepoConfig
) -> None:
    ERROR_MSG_PREFIX = "Unable to infer DataSource timestamp_field"
    for data_source in data_sources:
        if isinstance(data_source, RequestSource):
            continue
        if isinstance(data_source, PushSource):
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
            else:
                raise RegistryInferenceFailure(
                    "DataSource",
                    f"""
                    DataSource inferencing of timestamp_field is currently only supported
                    for FileSource, SparkSource, BigQuerySource, RedshiftSource, and SnowflakeSource.
                    Attempting to infer from {data_source}.
                    """,
                )
            #  for informing the type checker
            assert (
                isinstance(data_source, FileSource)
                or isinstance(data_source, BigQuerySource)
                or isinstance(data_source, RedshiftSource)
                or isinstance(data_source, SnowflakeSource)
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
    fvs: Union[List[FeatureView], List[StreamFeatureView]],
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
    entity_name_to_join_keys_map = {e.name: e.join_keys for e in entities}

    for fv in fvs:
        join_keys = set(
            [
                join_key
                for entity_name in fv.entities
                for join_key in entity_name_to_join_keys_map[entity_name]
            ]
        )

        # Fields whose names match a join key are considered to be entity columns; all
        # other fields are considered to be feature columns.
        for field in fv.schema:
            if field.name in join_keys:
                # Do not override a preexisting field with the same name.
                if field.name not in [
                    entity_column.name for entity_column in fv.entity_columns
                ]:
                    fv.entity_columns.append(field)
            else:
                if field.name not in [feature.name for feature in fv.features]:
                    fv.features.append(field)

        # Since the `value_type` parameter has not yet been fully deprecated for
        # entities, we respect the `value_type` attribute if it still exists.
        for entity_name in fv.entities:
            entity = entity_name_to_entity_map[entity_name]
            if (
                entity.join_key
                not in [entity_column.name for entity_column in fv.entity_columns]
                and entity.value_type != ValueType.UNKNOWN
            ):
                fv.entity_columns.append(
                    Field(
                        name=entity.join_key,
                        dtype=from_value_type(entity.value_type),
                    )
                )

        # Infer a dummy entity column for entityless feature views.
        if len(fv.entities) == 1 and fv.entities[0] == DUMMY_ENTITY_NAME:
            fv.entity_columns.append(Field(name=DUMMY_ENTITY_ID, dtype=String))

        # Run inference for entity columns if there are fewer entity fields than expected.
        num_expected_join_keys = sum(
            [
                len(entity_name_to_join_keys_map[entity_name])
                for entity_name in fv.entities
            ]
        )
        run_inference_for_entities = len(fv.entity_columns) < num_expected_join_keys

        # Run inference for feature columns if there are no feature fields.
        run_inference_for_features = len(fv.features) == 0

        if run_inference_for_entities or run_inference_for_features:
            _infer_features_and_entities(
                fv,
                join_keys,
                run_inference_for_features,
                config,
            )

            if not fv.features:
                raise RegistryInferenceFailure(
                    "FeatureView",
                    f"Could not infer Features for the FeatureView named {fv.name}.",
                )


def _infer_features_and_entities(
    fv: FeatureView,
    join_keys: Set[str],
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
    columns_to_exclude = {
        fv.batch_source.timestamp_field,
        fv.batch_source.created_timestamp_column,
    }
    for column in columns_to_exclude:
        if column in fv.batch_source.field_mapping:
            columns_to_exclude.remove(column)
            columns_to_exclude.add(fv.batch_source.field_mapping[column])

    table_column_names_and_types = fv.batch_source.get_table_column_names_and_types(
        config
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
                fv.entity_columns.append(field)
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
