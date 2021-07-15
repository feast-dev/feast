import re
from typing import List

from feast import BigQuerySource, Entity, FileSource, RedshiftSource
from feast.data_source import DataSource
from feast.errors import RegistryInferenceFailure
from feast.feature_view import FeatureView
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


def update_entities_with_inferred_types_from_feature_views(
    entities: List[Entity], feature_views: List[FeatureView], config: RepoConfig
) -> None:
    """
    Infer entity value type by examining schema of feature view input sources
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

        col_names_and_types = view.input.get_table_column_names_and_types(config)
        for entity_name in view.entities:
            if entity_name in incomplete_entities:
                # get entity information from information extracted from the view input source
                extracted_entity_name_type_pairs = list(
                    filter(lambda tup: tup[0] == entity_name, col_names_and_types)
                )
                if len(extracted_entity_name_type_pairs) == 0:
                    # Doesn't mention inference error because would also be an error without inferencing
                    raise ValueError(
                        f"""No column in the input source for the {view.name} feature view matches
                        its entity's name."""
                    )

                entity = incomplete_entities[entity_name]
                inferred_value_type = view.input.source_datatype_to_feast_value_type()(
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
    ERROR_MSG_PREFIX = "Unable to infer DataSource event_timestamp_column"

    for data_source in data_sources:
        if (
            data_source.event_timestamp_column is None
            or data_source.event_timestamp_column == ""
        ):
            # prepare right match pattern for data source
            ts_column_type_regex_pattern = ""
            if isinstance(data_source, FileSource):
                ts_column_type_regex_pattern = r"^timestamp"
            elif isinstance(data_source, BigQuerySource):
                ts_column_type_regex_pattern = "TIMESTAMP|DATETIME"
            elif isinstance(data_source, RedshiftSource):
                ts_column_type_regex_pattern = "TIMESTAMP[A-Z]*"
            else:
                raise RegistryInferenceFailure(
                    "DataSource",
                    """
                    DataSource inferencing of event_timestamp_column is currently only supported
                    for FileSource and BigQuerySource.
                    """,
                )
            #  for informing the type checker
            assert isinstance(data_source, FileSource) or isinstance(
                data_source, BigQuerySource
            )

            # loop through table columns to find singular match
            event_timestamp_column, matched_flag = None, False
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
                    event_timestamp_column = col_name
            if matched_flag:
                data_source.event_timestamp_column = event_timestamp_column
            else:
                raise RegistryInferenceFailure(
                    "DataSource",
                    f"""
                    {ERROR_MSG_PREFIX} due to an absence of columns that satisfy the criteria.
                    """,
                )
