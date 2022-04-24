import re
from typing import List

from feast import BigQuerySource, Entity, FileSource, RedshiftSource, SnowflakeSource
from feast.data_source import DataSource, PushSource, RequestSource
from feast.errors import RegistryInferenceFailure
from feast.feature_view import FeatureView
from feast.field import Field, from_value_type
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


def update_entities_with_inferred_types_from_feature_views(
    entities: List[Entity], feature_views: List[FeatureView], config: RepoConfig
) -> None:
    """
    Infers the types of the entities by examining the schemas of feature view batch sources.

    Args:
        entities: The entities to be updated.
        feature_views: A list containing feature views associated with the entities.
        config: The config for the current feature store.
    """
    incomplete_entities = {
        entity.name: entity
        for entity in entities
        if entity.value_type == ValueType.UNKNOWN
    }
    incomplete_entities_keys = incomplete_entities.keys()

    for view in feature_views:
        if not (incomplete_entities_keys & set(view.entities)):
            continue  # skip if view doesn't contain any entities that need inference

        col_names_and_types = list(
            view.batch_source.get_table_column_names_and_types(config)
        )
        for entity_name in view.entities:
            if entity_name in incomplete_entities:
                entity = incomplete_entities[entity_name]

                # get entity information from information extracted from the view batch source
                extracted_entity_name_type_pairs = list(
                    filter(lambda tup: tup[0] == entity.join_key, col_names_and_types,)
                )
                if len(extracted_entity_name_type_pairs) == 0:
                    # Doesn't mention inference error because would also be an error without inferencing
                    raise ValueError(
                        f"""No column in the batch source for the {view.name} feature view matches
                        its entity's name."""
                    )

                inferred_value_type = view.batch_source.source_datatype_to_feast_value_type()(
                    extracted_entity_name_type_pairs[0][1]
                )

                if (
                    entity.value_type != ValueType.UNKNOWN
                    and entity.value_type != inferred_value_type
                ) or (len(extracted_entity_name_type_pairs) > 1):
                    raise RegistryInferenceFailure(
                        "Entity",
                        f"""Entity value_type inference failed for {entity_name} entity.
                        Multiple viable matches.
                        """,
                    )

                entity.value_type = inferred_value_type


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


def update_feature_views_with_inferred_features(
    fvs: List[FeatureView], entities: List[Entity], config: RepoConfig
) -> None:
    """
    Infers the set of features associated to each FeatureView and updates the FeatureView with those features.
    Inference occurs through considering each column of the underlying data source as a feature except columns that are
    associated with the data source's timestamp columns and the FeatureView's entity columns.

    Args:
        fvs: The feature views to be updated.
        entities: A list containing entities associated with the feature views.
        config: The config for the current feature store.
    """
    entity_name_to_join_key_map = {entity.name: entity.join_key for entity in entities}
    join_keys = entity_name_to_join_key_map.values()

    for fv in fvs:
        # First drop all Entity fields. Then infer features if necessary.
        fv.schema = [field for field in fv.schema if field.name not in join_keys]
        fv.features = [field for field in fv.features if field.name not in join_keys]

        if not fv.features:
            columns_to_exclude = {
                fv.batch_source.timestamp_field,
                fv.batch_source.created_timestamp_column,
            } | {
                entity_name_to_join_key_map[entity_name] for entity_name in fv.entities
            }

            if fv.batch_source.timestamp_field in fv.batch_source.field_mapping:
                columns_to_exclude.add(
                    fv.batch_source.field_mapping[fv.batch_source.timestamp_field]
                )
            if (
                fv.batch_source.created_timestamp_column
                in fv.batch_source.field_mapping
            ):
                columns_to_exclude.add(
                    fv.batch_source.field_mapping[
                        fv.batch_source.created_timestamp_column
                    ]
                )

            for (
                col_name,
                col_datatype,
            ) in fv.batch_source.get_table_column_names_and_types(config):
                if col_name not in columns_to_exclude and not re.match(
                    "^__|__$",
                    col_name,  # double underscores often signal an internal-use column
                ):
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
                    # Note that schema and features are two different attributes of a
                    # FeatureView, and that features should be present in both.
                    fv.schema.append(field)
                    fv.features.append(field)

            if not fv.features:
                raise RegistryInferenceFailure(
                    "FeatureView",
                    f"Could not infer Features for the FeatureView named {fv.name}.",
                )
