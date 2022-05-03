import re
from typing import List

from feast import BigQuerySource, Entity, FileSource, RedshiftSource, SnowflakeSource
from feast.data_source import DataSource, PushSource, RequestSource
from feast.errors import RegistryInferenceFailure
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_NAME, FeatureView
from feast.field import Field, from_value_type
from feast.repo_config import RepoConfig
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
            ts_column_type_regex_pattern = ""
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
            timestamp_field, matched_flag = None, False
            for (
                col_name,
                col_datatype,
            ) in data_source.get_table_column_names_and_types(config):
                if re.match(ts_column_type_regex_pattern, col_datatype):
                    if matched_flag:
                        raise RegistryInferenceFailure(
                            "DataSource",
                            f"""
                            {ERROR_MSG_PREFIX} due to multiple possible columns satisfying
                            the criteria. {ts_column_type_regex_pattern} {col_name}
                            """,
                        )
                    matched_flag = True
                    timestamp_field = col_name
            if matched_flag:
                assert timestamp_field
                data_source.timestamp_field = timestamp_field
            else:
                raise RegistryInferenceFailure(
                    "DataSource",
                    f"""
                    {ERROR_MSG_PREFIX} due to an absence of columns that satisfy the criteria.
                    """,
                )


def update_feature_views_with_inferred_features_and_entities(
    fvs: List[FeatureView], entities: List[Entity], config: RepoConfig
) -> None:
    """
    Infers the set of features and entities associated with each feature view and updates
    the feature view with those features and entities. Columns whose names match a join key
    of an entity are considered to be entity columns; all other columns except designated
    timestamp columns, are considered to be feature columns.

    Args:
        fvs: The feature views to be updated.
        entities: A list containing entities associated with the feature views.
        config: The config for the current feature store.
    """
    entity_name_to_entity_map = {e.name: e for e in entities}
    entity_name_to_join_keys_map = {e.name: e.join_keys for e in entities}
    all_join_keys = [
        join_key
        for join_key_list in entity_name_to_join_keys_map.values()
        for join_key in join_key_list
    ]

    for fv in fvs:
        # Fields whose names match a join key are considered to be entity columns; all
        # other fields are considered to be feature columns.
        for field in fv.schema:
            if field.name in all_join_keys:
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
                entity_name
                not in [entity_column.name for entity_column in fv.entity_columns]
                and entity.value_type != ValueType.UNKNOWN
            ):
                fv.entity_columns.append(
                    Field(
                        name=entity.join_key, dtype=from_value_type(entity.value_type),
                    )
                )

        # Handle EFV separately here. Specifically, that means if we have an EFV,
        # we need to add a field to entity_columns.
        if len(fv.entities) == 1 and fv.entities[0] == DUMMY_ENTITY_NAME:
            fv.entity_columns.append(Field(name=DUMMY_ENTITY_ID, dtype=String))

        # Run inference if either (a) there are fewer entity fields than expected or
        # (b) there are no feature fields.
        run_inference = len(fv.features) == 0
        num_expected_join_keys = sum(
            [
                len(entity_name_to_join_keys_map[entity_name])
                for entity_name in fv.entities
            ]
        )
        if len(fv.entity_columns) < num_expected_join_keys:
            run_inference = True

        if run_inference:
            join_keys = set(
                [
                    join_key
                    for entity_name in fv.entities
                    for join_key in entity_name_to_join_keys_map[entity_name]
                ]
            )

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
                            fv.batch_source.source_datatype_to_feast_value_type()(
                                col_datatype
                            )
                        ),
                    )
                    if field.name not in [
                        entity_column.name for entity_column in fv.entity_columns
                    ]:
                        fv.entity_columns.append(field)
                elif not re.match(
                    "^__|__$", col_name
                ):  # double underscores often signal an internal-use column
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

            if not fv.features:
                raise RegistryInferenceFailure(
                    "FeatureView",
                    f"Could not infer Features for the FeatureView named {fv.name}.",
                )
